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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.spi.ConfigSource;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;


/**
 * Configuration class that holds transaction mapping per Kafka topic.
 *
 * <p>
 * This class uses MicroProfile Config to read configuration from microprofile-config.properties.
 * It serves a similar purpose to Spring Boot's @ConfigurationProperties.
 * </p>
 *
 */
@ApplicationScoped
public class KafkaConfig
{

    // Map of topic -> CICS transaction ID
    private final Map<String, String> topicTranMap = new HashMap<>();

    @Inject
    private Config config;


    /**
     * Builds Kafka consumer properties for a given topic using MicroProfile Config.
     *
     * <p>
     * This method reads configuration from microprofile-config.properties and constructs
     * a Properties object suitable for creating a KafkaConsumer. 
     *
     * @param topicName the Kafka topic name
     * @return Properties object with Kafka consumer configuration
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
                // Add global Kafka properties (e.g., bootstrap.servers)
                if (key.startsWith("bootstrap.servers"))
                {
                    props.put(key, config.getValue(key, String.class));
                }

                // Add topic-specific properties
                if (key.startsWith(topicPrefix))
                {
                    props.put(key.substring(topicPrefix.length()), config.getValue(key, String.class));
                }

                // Build CICS transaction mapping
                if (key.startsWith(cicsPrefix))
                {
                    topicTranMap.put(key.substring(cicsPrefix.length()), config.getValue(key, String.class));
                }
            }
        }

        return props;
    }


    /**
     * Returns the CICS transaction ID for a given topic.
     *
     * @param topic the Kafka topic name
     * @return CICS transaction ID, defaults to "CJSU" if not configured
     */
    public String getTranIdForTopic(String topic)
    {
        return topicTranMap.getOrDefault(topic, "CJSU");
    }
}

// Made with Bob
