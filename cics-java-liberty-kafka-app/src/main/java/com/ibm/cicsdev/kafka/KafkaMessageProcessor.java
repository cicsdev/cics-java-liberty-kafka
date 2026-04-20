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

import java.util.logging.Level;
import java.util.logging.Logger;

import com.ibm.cics.server.CICSTransactionRunnable;
import com.ibm.cics.server.InvalidRequestException;
import com.ibm.cics.server.Task;

import jakarta.annotation.Resource;
import jakarta.enterprise.concurrent.ManagedExecutorService;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;


/**
 * KafkaMessageProcessor executes incoming Kafka messages asynchronously in a CICS transaction context.
 *
 * <p>
 * <b>Key Responsibilities:</b>
 * <ul>
 * <li>Submits messages to Liberty's ManagedExecutorService for async processing</li>
 * <li>Wraps each message in a CICSTransactionRunnable for CICS transaction context</li>
 * <li>Maps topics to CICS transaction IDs dynamically</li>
 * <li>Ensures messages run on CICS-aware threads</li>
 * </ul>
 * </p>
 */
@ApplicationScoped
public class KafkaMessageProcessor
{

    @Resource(lookup = "java:comp/DefaultManagedExecutorService")
    private ManagedExecutorService executor;

    @Inject
    KafkaConfig config;

    private static final Logger LOG = Logger.getLogger(KafkaMessageProcessor.class.getName());


    /**
     * Processes a Kafka message asynchronously in a CICS transaction context.
     *
     * <p>
     * This method submits the message to Liberty's ManagedExecutorService, which creates
     * CICS-aware threads. The message is wrapped in a CICSTransactionRunnable to ensure
     * it executes within a CICS transaction with the appropriate transaction ID.
     * </p>
     *
     * @param topic Kafka topic name
     * @param message Kafka message payload
     */
    public void processAsynchronous(String topic, String message)
    {
        LOG.info(() -> "Received message from topic " + topic + " having message : " + message);
        executor.submit(new KafkaCICSTransactionRunnable(topic, message));
    }


    /**
     * Runnable wrapper that executes a Kafka message within a CICS transaction.
     */
    private class KafkaCICSTransactionRunnable implements CICSTransactionRunnable
    {
        private final String kafkaMessage;
        private final String topic;


        public KafkaCICSTransactionRunnable(String topic, String kafkaMessage)
        {
            this.kafkaMessage = kafkaMessage;
            this.topic = topic;
        }


        /**
         * Executes the message processing logic within a CICS transaction.
         */
        @Override
        public void run()
        {
            Task task = Task.getTask();
            if (task == null)
            {
                LOG.severe(() -> ("ERROR: Could not obtain CICS Task"));
                return;
            }

            // Log the user ID under which this transaction is running
            try
            {
                String userId = task.getUSERID();
                LOG.info("Task USERID = " + userId);
            }
            catch (InvalidRequestException e)
            {
                LOG.log(Level.FINE, "Failed to get userid", e);
            }

            // Process the message
            LOG.info(() -> ("DEBUG: Topic = " + topic));
            LOG.info(() -> ("DEBUG: Finished processing Kafka message in thread: " + Thread.currentThread().getName()
                + " " + kafkaMessage));
        }


        /**
         * Returns the CICS transaction ID for this message.
         *
         * <p>
         * The transaction ID is determined by looking up the topic in KafkaConfig.
         * If no mapping exists, the default transaction ID "CJSU" is used.
         * </p>
         *
         * @return CICS transaction ID (e.g., "KAFK", "KAF1", or "CJSU")
         */
        @Override
        public String getTranid()
        {
            return config.getTranIdForTopic(topic);
        }
    }
}

