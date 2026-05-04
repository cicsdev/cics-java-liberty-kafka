/* Licensed Materials - Property of IBM                               */
/*                                                                    */
/* SAMPLE                                                             */
/*                                                                    */
/* (c) Copyright IBM Corp. 2016, 2025 All Rights Reserved             */
/*                                                                    */
/* US Government Users Restricted Rights - Use, duplication or        */
/* disclosure restricted by GSA ADP Schedule Contract with IBM Corp   */
/*                                                                    */
package com.ibm.cicsdev.kafka;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.security.auth.Subject;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import com.ibm.websphere.security.WSSecurityException;
import com.ibm.websphere.security.auth.WSSubject;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;


/**
 * KafkaConsumerService manages **per-topic Kafka consumers** in Liberty.
 *
 * <p>
 * <b>Features:</b>
 * <ul>
 * <li>Dynamic start/stop of consumers per topic</li>
 * <li>Sets Liberty RunAs Subject to ensure proper CICS transaction identity</li>
 * <li>Handles incoming messages asynchronously via KafkaMessageProcessor</li>
 * </ul>
 * </p>
 */
@ApplicationScoped
public class KafkaConsumerService
{
    @Inject
    KafkaConfig config;

    @Inject
    KafkaController control;

    @Inject
    KafkaMessageProcessor processor;

    private static final Logger LOG = Logger.getLogger(KafkaConsumerService.class.getName());

    // Guard: one-time RunAs initialisation per consumer thread
    private final ThreadLocal<Boolean> runAsInitialized = ThreadLocal.withInitial(() -> false);
    private final ThreadLocal<Subject> previousRunAs = new ThreadLocal<>();

    /** Topic → Running Flag */
    private final Map<String, AtomicBoolean> runningTopics = new ConcurrentHashMap<>();

    /** Topic → KafkaConsumer */
    private final Map<String, KafkaConsumer<String, String>> consumerMap = new ConcurrentHashMap<>();


    /**
     * Starts consuming messages for a given topic under the caller's Liberty Subject.
     *
     * <p>
     * Uses AtomicBoolean and ConcurrentHashMap to prevent duplicate consumers for the same topic.
     * Multiple topics can run concurrently, each on its own thread.
     * </p>
     *
     * <p>
     * <b>Security Context:</b><br>
     * The provided Subject is stored and used by handleMessage() to establish RunAs identity
     * on the consumer thread before processing messages.
     * </p>
     *
     * @param topic the Kafka topic to listen on
     * @param subject Liberty Subject of the authenticated caller
     */
    public void startConsuming(String topic, Subject subject)
    {
        // Atomic check to prevent duplicate listeners for the same topic
        AtomicBoolean flag = new AtomicBoolean(true);
        AtomicBoolean existing = runningTopics.putIfAbsent(topic, flag);

        if (existing != null)
        {
            LOG.info("Consumer already running for topic: " + topic);
            return;
        }

        // Create and start a new consumer thread for this topic
        Thread t = new Thread(() ->
        {
            KafkaConsumer<String, String> consumer = null;
            try
            {
                // Build Kafka consumer properties from Config
                Properties props = config.buildKafkaPropertiesForTopic(topic);

                if (props.isEmpty())
                {
                    LOG.warning("No Kafka properties found for topic: " + topic);
                    return;
                }

                LOG.info("Kafka props for " + topic + ": " + props);

                // Create and subscribe the Kafka consumer
                consumer = new KafkaConsumer<>(props);
                consumer.subscribe(Collections.singletonList(topic));

                // Store consumer reference
                consumerMap.put(topic, consumer);

                // Poll loop: continues until stop() is called
                while (runningTopics.get(topic).get())
                {
                    // Poll for new messages (200ms timeout)
                    ConsumerRecords<String, String> records = consumer.poll(java.time.Duration.ofMillis(200));

                    // Process each message in the batch
                    for (ConsumerRecord<String, String> r : records)
                    {
                        try
                        {
                            handleMessage(topic, r.value());
                        }
                        catch (Exception ex)
                        {
                            LOG.severe("Error processing message: " + ex);
                        }
                    }
                }
            }
            catch (WakeupException we)
            {
                LOG.info("Consumer wakeup received for topic=" + topic);
            }
            catch (Exception e)
            {
                LOG.severe("Error in Kafka consumer for topic " + topic + ": " + e.getMessage());
            }
            finally
            {
                // Clean up resources
                if (consumer != null)
                {
                    try
                    {
                        consumer.close();
                    }
                    catch (Exception ignored)
                    {
                        LOG.warning("Error closing consumer: " + ignored.getMessage());
                    }
                }

                // Restore previous RunAs Subject if it was set
                restoreRunAsIfInitialized();

                // Remove topic from active maps
                runningTopics.remove(topic);
                consumerMap.remove(topic);

                LOG.info("Consumer closed for topic: " + topic);
            }
        });
        t.start();
    }


    /**
     * Stops consumption for a given topic.
     *
     * <p>
     * This method signals the consumer thread to stop polling. The consumer will finish processing
     * the current batch before the thread exits.
     * </p>
     *
     * @param topic Kafka topic to stop
     */
    public void stop(String topic)
    {
        AtomicBoolean running = runningTopics.get(topic);

        if (running == null || !running.get())
        {
            LOG.info("Consumer not running for topic: " + topic);
            return;
        }

        LOG.info("Stopping consumer for topic: " + topic);

        // Signal the poll loop to exit
        running.set(false);

        // Interrupt any in-progress poll() call
        KafkaConsumer<String, String> consumer = consumerMap.get(topic);
        if (consumer != null)
        {
            try
            {
                consumer.wakeup(); // Forces poll() to throw WakeupException
            }
            catch (Exception ignored)
            {
                LOG.warning("Error during consumer wakeup: " + ignored.getMessage());
            }
        }
    }


    /**
     * Handles incoming Kafka message with security context management.
     *
     * <p>
     * <b>Process Flow:</b>
     * <ol>
     * <li>Retrieves the Subject for this topic from the controller</li>
     * <li>Sets RunAs Subject on the consumer thread</li>
     * <li>Delegates message processing to KafkaMessageProcessor</li>
     * </ol>
     * </p>
     *
     * @param topic Kafka topic name
     * @param message Kafka message payload
     */
    void handleMessage(String topic, String message)
    {
        // Retrieve the Subject associated with this topic
        Subject subject = control.getActiveTopics().get(topic);

        if (subject == null)
        {
            LOG.info("Skipping message for inactive topic " + topic);
            LOG.warning(() -> "No Subject for topic '" + topic + "' — skipping message.");
            return;
        }

        // Set RunAs Subject once per consumer thread
        if (!runAsInitialized.get())
        {
            try
            {
                // Save previous RunAs Subject for restoration on thread exit
                Subject prev = WSSubject.getRunAsSubject();
                previousRunAs.set(prev);

                WSSubject.setRunAsSubject(subject);
                runAsInitialized.set(true);

                LOG.fine(() -> "RunAs set for topic '" + topic + "' on consumer thread.");
            }
            catch (WSSecurityException e)
            {
                LOG.log(Level.SEVERE, "Failed to set RunAsSubject for topic '" + topic + "': " + e.getMessage(), e);
                return;
            }
        }

        // Submit message to async processor
        processor.processAsynchronous(topic, message);
    }


    /**
     * Restores the previous RunAs Subject and clears ThreadLocal flags.
     */
    public void restoreRunAsIfInitialized()
    {
        if (runAsInitialized.get())
        {
            try
            {
                Subject prev = previousRunAs.get();
                if (prev != null)
                {
                    WSSubject.setRunAsSubject(prev);
                }
            }
            catch (WSSecurityException e)
            {
                LOG.log(Level.WARNING, "Failed to restore previous RunAsSubject: " + e.getMessage(), e);
            }
            finally
            {
                runAsInitialized.remove();
                previousRunAs.remove();
            }
        }
    }
}
