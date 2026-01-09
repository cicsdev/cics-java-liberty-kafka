/* Licensed Materials - Property of IBM                               */
/*                                                                    */
/* SAMPLE                                                             */
/*                                                                    */
/* (c) Copyright IBM Corp. 2016, 2025 All Rights Reserved             */
/*                                                                    */
/* US Government Users Restricted Rights - Use, duplication or        */
/* disclosure restricted by GSA ADP Schedule Contract with IBM Corp   */
/*                                                                    */
package com.example.kafkaliberty;

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
 * Features: <br>
 * - Starts/stops consumers dynamically per topic. <br>
 * - Sets Liberty RunAs Subject to ensure proper CICS transaction identity. <br>
 * - Handles incoming messages asynchronously via KafkaMessageProcessor.
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
     * Starts consuming messages for a given topic under the caller’s Liberty Subject.
     *
     * @param topic
     *            the Kafka topic to listen on
     * @param subject
     *            Liberty Subject of the caller
     */
    public void startConsuming(String topic, Subject subject)
    {
        // Atomic check to prevent duplicate listeners
        AtomicBoolean flag = new AtomicBoolean(true);
        AtomicBoolean existing = runningTopics.putIfAbsent(topic, flag);

        if (existing != null)
        {
            LOG.info("Consumer already running for topic: " + topic);
            return;
        }

        Thread t = new Thread(() ->
        {
            KafkaConsumer<String, String> consumer = null;
            try
            {
                Properties props = config.buildKafkaPropertiesForTopic(topic);

                if (props.isEmpty())
                {
                    LOG.warning("No Kafka properties found for topic: " + topic);
                    return;
                }

                LOG.info("Kafka props for " + topic + ": " + props);

                consumer = new KafkaConsumer<>(props);
                consumer.subscribe(Collections.singletonList(topic));

                consumerMap.put(topic, consumer);

                while (runningTopics.get(topic).get())
                {
                    ConsumerRecords<String, String> records = consumer.poll(java.time.Duration.ofMillis(200));

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
                if (consumer != null)
                {
                    try
                    {
                        consumer.close();
                    }
                    catch (Exception ignored)
                    {
                        // TODO: Log exception
                    }
                }

                restoreRunAsIfInitialized();

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
     * @param topic
     *            Kafka topic to stop
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

        running.set(false);

        KafkaConsumer<String, String> consumer = consumerMap.get(topic);
        if (consumer != null)
        {
            try
            {
                consumer.wakeup(); // force poll() to exit NOW
            }
            catch (Exception ignored)
            {
                // TODO: log exception
            }
        }
    }


    /**
     * Handles incoming message:<br>
     * - Checks if the topic is active. <br>
     * - Sets RunAs Subject for the consumer thread.<br>
     * - Delegates to KafkaMessageProcessor asynchronously.
     */
    void handleMessage(String topic, String message)
    {

        Subject subject = control.getActiveTopics().get(topic);

        if (subject == null)
        {
            LOG.info("Skipping message for inactive topic " + topic);
            LOG.warning(() -> "No Subject for topic '" + topic + "' — skipping message.");
            return;
        }

        if (!runAsInitialized.get())
        {
            try
            {
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

        // Submit message to your async processor
        processor.processAsynchronous(topic, message);
    }


    /**
     * Restore previous RunAs subject and clear thread-local flags.
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
