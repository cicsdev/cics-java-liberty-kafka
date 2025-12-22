package com.example.kafkaliberty;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.spi.ConfigSource;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * Holds transaction mapping per Kafka topic. Equivalent of
 * Spring's @ConfigurationProperties("cics.transaction")
 */
@ApplicationScoped
public class KafkaConfig
{

	// Map: topic -> CICS transaction ID
	private final Map<String, String> topicTranMap = new HashMap<>();

	@Inject
	private Config config;

	/**
	 * Build Kafka consumer Properties dynamically using MP Config.
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
				if(key.startsWith("bootstrap.servers")) 
				{
					props.put(key,config.getValue(key, String.class));
				}
				
				// 2. Incoming topic-specific props
				if (key.startsWith(topicPrefix)) 
				{
					props.put(key.substring(topicPrefix.length()),config.getValue(key, String.class));
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
	 */
	public String getTranIdForTopic(String topic) 
	{
		return topicTranMap.getOrDefault(topic, "CJSU");
	}
}
