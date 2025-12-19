package com.example.kafkaliberty;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.ibm.cics.server.CICSTransactionRunnable;
import com.ibm.cics.server.InvalidRequestException;
import com.ibm.cics.server.Task;

import jakarta.annotation.Resource;
import jakarta.enterprise.concurrent.ManagedExecutorService;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class KafkaMessageProcessor 
{

    @Resource(lookup = "java:comp/DefaultManagedExecutorService")
    private ManagedExecutorService executor;
    
    @Inject
    KafkaConfig config;
    
    private static final Logger LOG = Logger.getLogger(KafkaMessageProcessor.class.getName());
    
    public void processAsynchronous(String topic, String message) 
    {        
        LOG.info(() -> "Received message from topic " +topic +" having message : "+ message);
        executor.submit(new KafkaCICSTransactionRunnable(topic,message)); 				   
    }
    
    
    /**
     * Runnable that wraps Kafka message and executes in a CICS-managed context.
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
        
        
        @Override
        public void run() 
        {        
        	Task task = Task.getTask();
            if (task == null) 
            {
            	LOG.severe(() -> ("ERROR: Could not obtain CICS Task"));                        
                return;
            }
            
            try 
            {
            	String userId = task.getUSERID();
            	LOG.info("Task USERID = " + userId);
            } 
            catch (InvalidRequestException e) 
            {
            	LOG.log(Level.FINE, "Failed to get userid", e);
            }
            
            LOG.info(() ->("DEBUG: Topic = " + topic));
            LOG.info(() ->("DEBUG: Finished processing Kafka message in thread: "
                    + Thread.currentThread().getName() + " " + kafkaMessage));  
        }

		@Override
        public String getTranid() 
		{
            return config.getTranIdForTopic(topic);
        }        
    }    
}

