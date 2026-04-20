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


/**
 * KafkaController provides REST endpoints to dynamically start and stop consumption of Kafka topics.
 *
 * <p>
 * This controller manages the lifecycle of Kafka consumers in a CICS Liberty environment using Jakarta EE.
 * </p>
 *
 * <p>
 * <b>Key Features:</b>
 * <ul>
 * <li>Dynamic topic activation/deactivation via REST endpoints</li>
 * <li>Per-topic security context management using Liberty Subject</li>
 * <li>Explicit RunAs identity propagation for CICS transactions</li>
 * </ul>
 * </p>
 *
 * <p>
 * <b>REST Endpoints:</b>
 * <ul>
 * <li><code>GET/POST /control/start?topic={topic}</code> - Activates a Kafka topic for consumption under the caller's security Subject</li>
 * <li><code>GET/POST /control/stop?topic={topic}</code> - Deactivates a Kafka topic and stops processing</li>
 * </ul>
 * </p>
 *
 * <p>
 * <b>Security Model:</b><br>
 * The controller captures the authenticated user's Liberty Subject when /start is called.
 * Different topics can run under different identities (different users call /start).
 * </p>
 *
 * <p>
 * <b>Alternative Security Approach:</b><br>
 * The commented-out LoginManager can be used for programmatic JAAS login with authData
 * from server.xml instead of capturing the HTTP caller's Subject. See LoginManager.java
 * and README.md for configuration details.
 * </p>
 */
@ApplicationScoped
@Path("/control") // Base path for REST API: /control/*
@DeclareRoles({ "cics-user" }) // Declares security roles recognized by Liberty
@RolesAllowed("cics-user") // Restricts access to authenticated users in "cics-user" role
public class KafkaController
{
    private static final Logger LOG = Logger.getLogger(KafkaController.class.getName());

    // Map to track active topics and their Subjects
    private final Map<String, Subject> activeTopics = new ConcurrentHashMap<>();

    @Inject
    private KafkaConsumerService kafkaConsumer;

    // Present in app but inactive; use in start() to enable programmatic login
    // @Autowired(required = false)
    // private LoginManager loginManager;

    
    /**
     * Start consumption for a single topic under the caller's Liberty Subject.
     *
     * <p>
     * The captured Subject represents the authenticated user who called this endpoint.
     * This identity will be used for all CICS transactions processing messages from this topic.
     * Different topics can run under different identities if different users call /start.
     * </p>
     *
     * @param topic Name of the Kafka topic to start consuming (required)
     * @return HTTP 200 with success message, or 400/401 on error
     */
    @GET
    @Path("/start")
    @Produces(MediaType.TEXT_PLAIN)
    public Response start(@QueryParam("topic") String topic)
    {
        // Validate 'topic' early to avoid NPE in the map.
        if (topic == null || topic.isBlank())
        {
            return Response.status(400).entity("ERROR: missing topic").build();
        }

        // Capture the caller's Liberty Subject
        Subject subject;
        try
        {
            subject = WSSubject.getCallerSubject();
            LOG.info(() -> ("DEBUG: Subject is: " + subject));
        }
        catch (WSSecurityException e)
        {
            return Response.status(401).entity("ERROR: cannot obtain caller subject: " + e).build();
        }

        // Verify authentication (Subject must not be null)
        if (subject == null)
        {
            return Response.status(401).entity("ERROR: unauthenticated request").build();
        }

        // Check if topic is already active
        if (activeTopics.containsKey(topic))
        {
            LOG.info(() -> ("Listener already running for topic " + topic));
            return Response.ok("Listener already running for topic=" + topic).build();
        }

        // Store the Subject for this topic
        activeTopics.put(topic, subject);

        // Start the Kafka consumer on a background thread
        kafkaConsumer.startConsuming(topic, subject);
        
        LOG.info(() -> ("Started listener for topic " + topic));
        return Response.ok("Started listener for topic=" + topic).build();
    }


    /**
     * Deactivate a Kafka topic and stop message consumption.
     *
     * <p>
     * This method stops the Kafka consumer thread for the specified topic.
     * The consumer will finish processing the current batch before the thread exits.
     * The Subject mapping for this topic is also removed.
     * </p>
     *
     * @param topic Kafka topic to stop (required)
     * @return HTTP 200 with success message, or 400 if topic parameter is missing
     */
    @GET
    @Path("/stop")
    @Produces(MediaType.TEXT_PLAIN)
    public Response stop(@QueryParam("topic") String topic)
    {
        // Validate input parameter
        if (topic == null || topic.isBlank())
        {
            return Response.status(400).entity("ERROR: missing topic").build();
        }

        // Remove the Subject mapping for this topic
        activeTopics.remove(topic);
        
        // Signal the consumer service to stop the consumer thread
        kafkaConsumer.stop(topic);

        LOG.info(() -> ("Stopped listener for topic " + topic));
        return Response.ok("Stopped listener for topic=" + topic).build();
    }

    /**
     * Returns the map of currently active topics and their associated Subjects.
     *
     * <p>
     * This map is used by KafkaConsumerService to retrieve the Subject for a topic
     * when processing messages.
     * </p>
     *
     * @return Map of topic names to Liberty Subjects
     */
    public Map<String, Subject> getActiveTopics()
    {
        return activeTopics;
    }
}
