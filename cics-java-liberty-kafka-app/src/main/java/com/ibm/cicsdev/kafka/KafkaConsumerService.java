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

import jakarta.annotation.Resource;
import jakarta.enterprise.concurrent.ManagedThreadFactory;
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
 * <li>Runs consumer loops on a container-managed thread (via ManagedThreadFactory)
 *     so CDI, JNDI, and classloader context propagate correctly to the
 *     background thread</li>
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

    /**
     * Container-managed thread factory. Unlike a ManagedExecutorService,
     * this creates a dedicated, non-pooled Thread per call — appropriate
     * here because each Kafka consumer loop is long-running (lives for as
     * long as the topic is active).
     */
    @Resource
    ManagedThreadFactory threadFactory;

    private static final Logger LOG = Logger.getLogger(KafkaConsumerService.class.getName());

    // Guard: one-time RunAs initialisation per consumer thread
    private final ThreadLocal<Boolean> runAsInitialized = ThreadLocal.withInitial(() -> false);
    private final ThreadLocal<Subject> previousRunAs = new ThreadLocal<>();

    /** Topic → Running Flag */
    private final Map<String, AtomicBoolean> runningTopics = new ConcurrentHashMap<>();

    /** Topic → KafkaConsumer */
    private final Map<String, KafkaConsumer<String, String>> consumerMap = new ConcurrentHashMap<>();
    
    /** Topic → consumer loop Thread, so stop() can join() it before returning */
    private final Map<String, Thread> consumerThreads = new ConcurrentHashMap<>();


    /**
     * Starts consuming messages for a given topic under the caller's Liberty Subject.
     *
     * <p>
     * Uses AtomicBoolean and ConcurrentHashMap to prevent duplicate consumers for the same topic.
     * Multiple topics can run concurrently, each on its own container-managed thread.
     * </p>
     *
     * <p>
     * <b>Security Context:</b><br>
     * The provided Subject is stored and used by handleMessage() to establish RunAs identity
     * on the consumer task before processing messages.
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

        // These proxy calls happen on the calling thread,
        // where CDI context is guaranteed active.
        final Map<String, Subject> activeTopics = control.getActiveTopics();
        final Properties props = config.buildKafkaPropertiesForTopic(topic);

        // Create the consumer loop's thread via the container's ManagedThreadFactory.
        Thread t = threadFactory.newThread(() ->
        {
            KafkaConsumer<String, String> consumer = null;
            try
            {
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
                            handleMessage(topic, r.value(), activeTopics);
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
                consumerThreads.remove(topic);

                LOG.info("Consumer closed for topic: " + topic);
            }
        });
        consumerThreads.put(topic, t);
        t.start();
    }


    /**
     * Stops consumption for a given topic.
     *
     * <p>
     * This method signals the consumer task to stop polling. The consumer will finish processing
     * the current batch before the task exits.
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
        
        // Block until the consumer thread has actually finished (including its
        // finally-block cleanup) so a fast subsequent start() for this topic
        // doesn't race against a not-yet-removed runningTopics/consumerMap entry.
        Thread consumerThread = consumerThreads.get(topic);
        if (consumerThread != null)
        {
            try
            {
                consumerThread.join(5000);
                if (consumerThread.isAlive())
                {
                    LOG.warning("Consumer thread for topic '" + topic + "' did not exit within "
                        + 5000 + "ms; a subsequent start() may still race.");
                }
            }
            catch (InterruptedException ie)
            {
                Thread.currentThread().interrupt();
                LOG.warning("Interrupted while waiting for consumer thread to stop for topic: " + topic);
            }
        }
    }


    /**
     * Handles incoming Kafka message with security context management.
     *
     * <p>
     * <b>Process Flow:</b>
     * <ol>
     * <li>Retrieves the Subject for this topic from the pre-captured map</li>
     * <li>Sets RunAs Subject on the consumer task's thread</li>
     * <li>Delegates message processing to the injected KafkaMessageProcessor</li>
     * </ol>
     * </p>
     *
     * @param topic Kafka topic name
     * @param message Kafka message payload
     * @param activeTopics pre-captured map of active topics to Subjects
     */
    void handleMessage(String topic, String message, Map<String, Subject> activeTopics)
    {
        // Use the pre-captured map
        Subject subject = activeTopics.get(topic);

        if (subject == null)
        {
            LOG.info("Skipping message for inactive topic " + topic);
            LOG.warning(() -> "No Subject for topic '" + topic + "' — skipping message.");
            return;
        }

        // Set RunAs Subject once per consumer task/thread
        if (!runAsInitialized.get())
        {
            try
            {
                // Save previous RunAs Subject for restoration on task exit
                Subject prev = WSSubject.getRunAsSubject();
                previousRunAs.set(prev);

                WSSubject.setRunAsSubject(subject);
                runAsInitialized.set(true);

                LOG.fine(() -> "RunAs set for topic '" + topic + "' on consumer task.");
            }
            catch (WSSecurityException e)
            {
                LOG.log(Level.SEVERE, "Failed to set RunAsSubject for topic '" + topic + "': " + e.getMessage(), e);
                return;
            }
        }

        // processor is a CDI proxy; this resolves correctly because the
        // ManagedThreadFactory-created thread carries propagated CDI context.
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