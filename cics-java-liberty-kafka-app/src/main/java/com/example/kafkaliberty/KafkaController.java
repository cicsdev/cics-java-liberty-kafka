package com.example.kafkaliberty;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import javax.security.auth.Subject;

import com.ibm.websphere.security.WSSecurityException;
import com.ibm.websphere.security.auth.WSSubject;

import jakarta.annotation.security.DeclareRoles;
import jakarta.annotation.security.RolesAllowed;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

@ApplicationScoped
@Path("/control")
@DeclareRoles({"cics-user"})
@RolesAllowed("cics-user")
public class KafkaController 
{
    private static final Logger LOG = Logger.getLogger(KafkaController.class.getName());

    // Map to track active topics and their Subjects
    private final Map<String, Subject> activeTopics = new ConcurrentHashMap<>();

    @Inject
	private KafkaConsumerService kafkaConsumer;
    
    // Present in app but inactive; use in start() to enable programmatic login
    //@Autowired(required = false)
    //private LoginManager loginManager;

    @GET
    @Path("/start")
    @Produces(MediaType.TEXT_PLAIN)
    /**
     * Start consumption for a single topic under the caller's Liberty Subject (JWT/OIDC/Basic).
     */
    public Response start(@QueryParam("topic") String topic) 
    {
        if (topic == null || topic.isBlank()) 
        {
            return Response.status(400).entity("ERROR: missing topic").build();
        }

        // Capture the caller’s Liberty Subject
        Subject subject;
        try 
        {
            subject = WSSubject.getCallerSubject();
            LOG.info(() ->("DEBUG: Subject is: " + subject));
        } 
        catch (WSSecurityException e) 
        {
            return Response.status(401).entity("ERROR: cannot obtain caller subject: " + e).build();
        }

        if (subject == null) 
        {
            return Response.status(401).entity("ERROR: unauthenticated request").build();
        }

        // Save caller subject
        activeTopics.put(topic, subject);
        
        kafkaConsumer.startConsuming(topic, subject);

        LOG.info(()-> ("Started listener for topic " + topic));
        return Response.ok("Started listener for topic=" + topic).build();
    }


    @GET
    @Path("/stop")
    @Produces(MediaType.TEXT_PLAIN)
    public Response stop(@QueryParam("topic") String topic) 
    {
        if (topic == null || topic.isBlank()) 
        {
            return Response.status(400).entity("ERROR: missing topic").build();
        }

        activeTopics.remove(topic);
        kafkaConsumer.stop(topic);

        LOG.info(()->("Stopped listener for topic " + topic));
        return Response.ok("Stopped listener for topic=" + topic).build();
    }


    public Map<String, Subject> getActiveTopics() 
    {
        return activeTopics;
    }
}
