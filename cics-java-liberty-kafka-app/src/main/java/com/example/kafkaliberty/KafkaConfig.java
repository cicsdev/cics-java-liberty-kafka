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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.spi.ConfigSource;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;


/**
 * Holds transaction mapping per Kafka topic. Equivalent of Spring's @ConfigurationProperties("cics.transaction")
 */
@ApplicationScoped
public class KafkaConfig
{

    // Map of topic -> CICS transaction ID
    private final Map<String, String> topicTranMap = new HashMap<>();

    @Inject
    private Config config;


    /**
     * Build Kafka consumer properties for a given topic using MP Config.
     *
     * @param topicName
     *            the Kafka topic
     * @return Properties object with bootstrap servers, deserializers, etc.
     */
    public Properties buildKafkaPropertiesForTopic(String topicName)
    {
        Properties props = new Properties();

        String topicPrefix = topicName + ".";
        String cicsPrefix = "cics.transaction.map.";

        // Iterate over all config sources
        for (ConfigSource source : config.getConfigSources())
        {
            for (String key : source.getPropertyNames())
            {
                // 1. Bootstrap - Server
                if (key.startsWith("bootstrap.servers"))
                {
                    props.put(key, config.getValue(key, String.class));
                }

                // 2. Incoming topic-specific props
                if (key.startsWith(topicPrefix))
                {
                    props.put(key.substring(topicPrefix.length()), config.getValue(key, String.class));
                }

                // 3. CICS transaction map properties
                if (key.startsWith(cicsPrefix))
                {
                    topicTranMap.put(key.substring(cicsPrefix.length()), config.getValue(key, String.class));
                }

            }
        }

        return props;
    }


    /**
     * Get transaction ID for a given topic.
     * 
     * @param topic
     *            the Kafka topic
     * @return transaction ID, defaults to "CJSU" if not configured
     */
    public String getTranIdForTopic(String topic)
    {
        return topicTranMap.getOrDefault(topic, "CJSU");
    }
}
