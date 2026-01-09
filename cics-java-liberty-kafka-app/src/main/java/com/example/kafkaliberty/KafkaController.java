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
 * - /control/start?topic={topic} activates a Kafka topic for consumption under the caller's security Subject. <br>
 * - /control/stop?topic={topic} deactivates a Kafka topic and stops processing.
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

    // ---------------------------------------------------------------
    // REST API to start consuming messages from a topic
    // ---------------------------------------------------------------


    @GET
    @Path("/start")
    @Produces(MediaType.TEXT_PLAIN)
    /**
     * Start consumption for a single topic under the caller's Liberty Subject (JWT/OIDC/Basic).
     * 
     * @param topic
     *            Name of the Kafka topic to start consuming
     * @return HTTP Response indicating success or failure
     */
    public Response start(@QueryParam("topic") String topic)
    {
        // Validate input
        if (topic == null || topic.isBlank())
        {
            return Response.status(400).entity("ERROR: missing topic").build();
        }

        // Capture the caller’s Liberty Subject
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

        // Verify authentication
        if (subject == null)
        {
            return Response.status(401).entity("ERROR: unauthenticated request").build();
        }

        // Activate topic by saving its Subject
        activeTopics.put(topic, subject);

        // Trigger the consumer service to start processing messages
        kafkaConsumer.startConsuming(topic, subject);

        LOG.info(() -> ("Started listener for topic " + topic));
        return Response.ok("Started listener for topic=" + topic).build();
    }


    @GET
    @Path("/stop")
    @Produces(MediaType.TEXT_PLAIN)
    /**
     * Deactivate a Kafka topic.
     * 
     * @param topic
     *            Kafka topic to stop
     * @return HTTP Response indicating success or failure
     */
    public Response stop(@QueryParam("topic") String topic)
    {
        if (topic == null || topic.isBlank())
        {
            return Response.status(400).entity("ERROR: missing topic").build();
        }

        activeTopics.remove(topic);
        kafkaConsumer.stop(topic);

        LOG.info(() -> ("Stopped listener for topic " + topic));
        return Response.ok("Stopped listener for topic=" + topic).build();
    }


    // ---------------------------------------------------------------
    // Accessor for active topics map
    // ---------------------------------------------------------------
    /**
     * @return a map of currently active topics and their associated Subjects
     */
    public Map<String, Subject> getActiveTopics()
    {
        return activeTopics;
    }
}
