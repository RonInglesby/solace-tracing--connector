package com.solace.connectors.tracing.internal;

import static org.mule.runtime.extension.api.annotation.param.MediaType.ANY;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.UUID;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TextMessage;

import com.solace.connectors.tracing.api.ConsumeAckMode;
import com.solace.connectors.tracing.api.DestinationType;
import com.solace.connectors.tracing.api.SolaceMessageAttributes;
import com.solace.connectors.tracing.internal.SolaceConnection.PublisherSession;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;

import org.mule.runtime.api.connection.ConnectionException;
import org.mule.runtime.api.metadata.MediaType;
import org.mule.runtime.api.metadata.TypedValue;
import org.mule.runtime.extension.api.annotation.error.Throws;
import org.mule.runtime.extension.api.annotation.param.Config;
import org.mule.runtime.extension.api.annotation.param.Connection;
import org.mule.runtime.extension.api.annotation.param.Content;
import org.mule.runtime.extension.api.annotation.param.NullSafe;
import org.mule.runtime.extension.api.annotation.param.Optional;
import org.mule.runtime.extension.api.annotation.param.display.DisplayName;
import org.mule.runtime.extension.api.annotation.param.display.Summary;
import org.mule.runtime.extension.api.exception.ModuleException;
import org.mule.runtime.extension.api.runtime.operation.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Publish, Consume, Publish consume (request-reply), Ack and Recover session.
 * Every outbound message carries the trace context (Solace-native + traceparent properties) and every received
 * message gets a CONSUMER span parented on the broker-updated context, so the hierarchy stays intact.
 */
public class SolaceOperations {

    private static final Logger LOGGER = LoggerFactory.getLogger(SolaceOperations.class);

    // ------------------------------------------------------------------------------------------ Publish

    /**
     * Publishes a text message to a topic or queue.
     *
     * @return traceparent, tracestate (of the publish span) and messageId
     */
    @DisplayName("Publish")
    @Throws(PublishErrorTypeProvider.class)
    public Map<String, String> publish(@Config SolaceTracingConfiguration config,
                                       @Connection SolaceConnection connection,
                                       @Summary("Topic or queue name, e.g. orders/created") String destination,
                                       @Optional(defaultValue = "TOPIC") DestinationType destinationType,
                                       @Content @Optional(defaultValue = "#[payload]") TypedValue<InputStream> body,
                                       @Optional @Summary("Parent trace context, e.g. #[attributes.traceparent]") String traceparent,
                                       @Optional String tracestate,
                                       @Optional String spanName,
                                       @Optional @Summary("Overrides the configuration's default service name") String serviceName,
                                       @Optional String correlationId,
                                       @Optional @Summary("Queue or topic replies should go to (sets JMSReplyTo)") String replyTo,
                                       @Optional(defaultValue = "QUEUE") DestinationType replyToType,
                                       @Optional(defaultValue = "true") boolean persistent,
                                       @Optional(defaultValue = "4") int priority,
                                       @Optional(defaultValue = "0") @Summary("0 = never expires") long timeToLive,
                                       @Optional(defaultValue = "true") @DisplayName("DMQ eligible") boolean dmqEligible,
                                       @Optional @NullSafe Map<String, String> userProperties) {

        Span span = Tracing.startSpan(config.tracer(serviceName),
                blank(spanName) ? destination + " send" : spanName,
                Tracing.extract(traceparent, tracestate), SpanKind.PRODUCER, destination, "send");

        PublisherSession publisher = null;
        boolean broken = false;
        try (Scope scope = span.makeCurrent()) {
            publisher = connection.borrowPublisher();
            Destination dest = Messages.destination(publisher.session, destinationType, destination);

            TextMessage message = publisher.session.createTextMessage(Messages.text(body));
            applyHeaders(message, publisher.session, userProperties, correlationId, replyTo, replyToType, dmqEligible);
            Map<String, String> out = Tracing.inject(Context.current(), message);

            publisher.producer.send(dest, message, deliveryMode(persistent), priority, timeToLive);

            String messageId = message.getJMSMessageID();
            out.put("messageId", messageId == null ? "" : messageId);
            if (messageId != null) {
                span.setAttribute(AttributeKey.stringKey("messaging.message.id"), messageId);
            }
            return out;
        } catch (IOException e) {
            fail(span, e);
            throw new ModuleException("Could not read the message body for " + destination + ": " + e.getMessage(),
                    SolaceError.PUBLISH, e);
        } catch (JMSException e) {
            broken = true;
            fail(span, e);
            throw jmsFailure("publishing to " + destination, e, connection, SolaceError.PUBLISH);
        } finally {
            if (publisher != null) {
                if (broken) {
                    connection.discardPublisher(publisher);
                } else {
                    connection.returnPublisher(publisher);
                }
            }
            span.end();
        }
    }

    // ------------------------------------------------------------------------------------------ Consume

    /**
     * Receives one message from a queue or topic. The receive span is parented on the message's trace context;
     * pass attributes.traceparent to later publishes to continue that trace.
     */
    @DisplayName("Consume")
    @org.mule.runtime.extension.api.annotation.param.MediaType(value = ANY, strict = false)
    @Throws(ConsumeErrorTypeProvider.class)
    public Result<String, SolaceMessageAttributes> consume(@Config SolaceTracingConfiguration config,
                                                           @Connection SolaceConnection connection,
                                                           @Summary("Queue or topic name") String destination,
                                                           @Optional(defaultValue = "QUEUE") DestinationType consumerType,
                                                           @Optional @Summary("JMS message selector") String selector,
                                                           @Optional(defaultValue = "10000") @Summary("ms to wait; -1 = forever, 0 = don't wait") long maximumWait,
                                                           @Optional(defaultValue = "AUTO") ConsumeAckMode ackMode,
                                                           @Optional(defaultValue = "application/json") String contentType,
                                                           @Optional String spanName,
                                                           @Optional @Summary("Overrides the configuration's default service name") String serviceName) {

        boolean manual = ackMode == ConsumeAckMode.MANUAL;
        Session session = null;
        MessageConsumer consumer = null;
        boolean keepOpen = false;
        try {
            connection.start();
            session = connection.createSession(manual ? Session.CLIENT_ACKNOWLEDGE : Session.AUTO_ACKNOWLEDGE);
            Destination dest = Messages.destination(session, consumerType, destination);
            consumer = blank(selector) ? session.createConsumer(dest) : session.createConsumer(dest, selector);

            Message message = Messages.receive(consumer, maximumWait);
            if (message == null) {
                throw new ModuleException("No message received from " + destination + " within " + maximumWait + " ms",
                        SolaceError.TIMEOUT);
            }

            SolaceMessageAttributes attributes = receiveSpan(config, serviceName,
                    blank(spanName) ? destination + " receive" : spanName, destination, message);

            if (manual) {
                attributes.setAckId(config.acks().register(new ConsumeAckHandle(session, consumer, message)));
                keepOpen = true;
            }
            return Result.<String, SolaceMessageAttributes>builder()
                    .output(Messages.body(message))
                    .mediaType(MediaType.parse(contentType))
                    .attributes(attributes)
                    .build();
        } catch (JMSException e) {
            throw jmsFailure("consuming from " + destination, e, connection, SolaceError.CONSUMING);
        } finally {
            if (!keepOpen) {
                closeQuietly(consumer, session);
            }
        }
    }

    // --------------------------------------------------------------------------- Publish consume (request-reply)

    /**
     * Sends a request and waits for the reply. Replies go to a temporary queue unless replyTo is given, in which
     * case the reply is matched on JMSCorrelationID. The request carries this operation's span as trace context,
     * and the reply's receive span is parented on the reply's own context, so the responder's spans sit in between.
     */
    @DisplayName("Publish consume")
    @org.mule.runtime.extension.api.annotation.param.MediaType(value = ANY, strict = false)
    @Throws(RequestReplyErrorTypeProvider.class)
    public Result<String, SolaceMessageAttributes> publishConsume(@Config SolaceTracingConfiguration config,
                                                                  @Connection SolaceConnection connection,
                                                                  @Summary("Topic or queue the request goes to") String destination,
                                                                  @Optional(defaultValue = "QUEUE") DestinationType destinationType,
                                                                  @Content @Optional(defaultValue = "#[payload]") TypedValue<InputStream> body,
                                                                  @Optional @Summary("Reply queue; empty = temporary queue") String replyTo,
                                                                  @Optional(defaultValue = "10000") @Summary("ms to wait for the reply; -1 = forever") long maximumWait,
                                                                  @Optional @Summary("Generated when empty") String correlationId,
                                                                  @Optional @Summary("Parent trace context, e.g. #[attributes.traceparent]") String traceparent,
                                                                  @Optional String tracestate,
                                                                  @Optional(defaultValue = "application/json") String contentType,
                                                                  @Optional(defaultValue = "true") boolean persistent,
                                                                  @Optional String spanName,
                                                                  @Optional @Summary("Overrides the configuration's default service name") String serviceName,
                                                                  @Optional @NullSafe Map<String, String> userProperties) {

        String requestSpanName = blank(spanName) ? destination + " request" : spanName;
        Span span = Tracing.startSpan(config.tracer(serviceName), requestSpanName,
                Tracing.extract(traceparent, tracestate), SpanKind.CLIENT, destination, "request");

        Session session = null;
        MessageConsumer consumer = null;
        TemporaryQueue temporaryQueue = null;
        try (Scope scope = span.makeCurrent()) {
            connection.start();
            session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            Destination dest = Messages.destination(session, destinationType, destination);
            String correlation = blank(correlationId) ? UUID.randomUUID().toString() : correlationId;

            Destination replyDestination;
            if (blank(replyTo)) {
                temporaryQueue = session.createTemporaryQueue();
                replyDestination = temporaryQueue;
                consumer = session.createConsumer(temporaryQueue);
            } else {
                replyDestination = session.createQueue(replyTo);
                consumer = session.createConsumer(replyDestination,
                        "JMSCorrelationID = '" + correlation.replace("'", "''") + "'");
            }

            TextMessage request = session.createTextMessage(Messages.text(body));
            for (Map.Entry<String, String> property : userProperties.entrySet()) {
                request.setStringProperty(property.getKey(), property.getValue());
            }
            request.setJMSCorrelationID(correlation);
            request.setJMSReplyTo(replyDestination);
            Tracing.inject(Context.current(), request);

            MessageProducer producer = session.createProducer(dest);
            producer.send(request, deliveryMode(persistent), Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            span.setAttribute(AttributeKey.stringKey("messaging.message.conversation_id"), correlation);

            Message reply = Messages.receive(consumer, maximumWait);
            if (reply == null) {
                span.setStatus(StatusCode.ERROR, "No reply within " + maximumWait + " ms");
                throw new ModuleException("No reply from " + destination + " within " + maximumWait + " ms",
                        SolaceError.TIMEOUT);
            }

            // Parent the reply span on the reply's context (responder -> broker); else on this request span.
            Tracing.Extracted extracted = Tracing.extract(reply);
            Context replyParent = extracted.isValid() ? extracted.context : Context.current();
            String replyName = Messages.name(replyDestination);
            Span replySpan = Tracing.startSpan(config.tracer(serviceName), destination + " reply", replyParent,
                    SpanKind.CONSUMER, replyName, "receive");
            replySpan.setAttribute(AttributeKey.stringKey("trace.context.source"), extracted.isValid() ? extracted.source : "request");
            Map<String, String> replyContext = Tracing.inject(Context.root().with(replySpan));
            replySpan.end();

            SolaceMessageAttributes attributes = Messages.attributes(reply, replyName, replyContext,
                    extracted.isValid() ? extracted.source : "request");
            return Result.<String, SolaceMessageAttributes>builder()
                    .output(Messages.body(reply))
                    .mediaType(MediaType.parse(contentType))
                    .attributes(attributes)
                    .build();
        } catch (IOException e) {
            fail(span, e);
            throw new ModuleException("Could not read the request body for " + destination + ": " + e.getMessage(),
                    SolaceError.PUBLISH, e);
        } catch (JMSException e) {
            fail(span, e);
            throw jmsFailure("in request-reply with " + destination, e, connection, SolaceError.CONSUMING);
        } finally {
            if (consumer != null) {
                try {
                    consumer.close();
                } catch (JMSException e) {
                    LOGGER.debug("Error closing reply consumer", e);
                }
            }
            if (temporaryQueue != null) {
                try {
                    temporaryQueue.delete();
                } catch (JMSException e) {
                    LOGGER.debug("Error deleting temporary queue", e);
                }
            }
            closeQuietly(null, session);
            span.end();
        }
    }

    // ------------------------------------------------------------------------------------------ Ack / Recover

    /**
     * Acknowledges a message received in MANUAL mode (listener or Consume).
     * For the listener, the ack is applied when the flow finishes.
     */
    @DisplayName("Ack")
    @Throws(AckErrorTypeProvider.class)
    public void ack(@Config SolaceTracingConfiguration config,
                    @Summary("attributes.ackId of the message") String ackId) {
        AckHandle handle = config.acks().remove(ackId);
        if (handle == null) {
            throw new ModuleException("No pending message for ackId " + ackId
                    + " (already acked or recovered, timed out, or not received in MANUAL mode)", SolaceError.ACK);
        }
        try {
            handle.ack();
        } catch (JMSException e) {
            throw new ModuleException("Could not acknowledge message " + ackId + ": " + e.getMessage(), SolaceError.ACK, e);
        }
    }

    /** Hands a message received in MANUAL mode back to the broker for redelivery. */
    @DisplayName("Recover session")
    @Throws(AckErrorTypeProvider.class)
    public void recoverSession(@Config SolaceTracingConfiguration config,
                               @Summary("attributes.ackId of the message") String ackId) {
        AckHandle handle = config.acks().remove(ackId);
        if (handle == null) {
            throw new ModuleException("No pending message for ackId " + ackId
                    + " (already acked or recovered, timed out, or not received in MANUAL mode)", SolaceError.ACK);
        }
        try {
            handle.recover();
        } catch (JMSException e) {
            throw new ModuleException("Could not recover message " + ackId + ": " + e.getMessage(), SolaceError.ACK, e);
        }
    }

    // ------------------------------------------------------------------------------------------ helpers

    /** Creates the CONSUMER span for a received message and returns attributes carrying its context. */
    private static SolaceMessageAttributes receiveSpan(SolaceTracingConfiguration config, String serviceName,
                                                       String name, String destination, Message message)
            throws JMSException {
        Tracing.Extracted extracted = Tracing.extract(message);
        Span span = Tracing.startSpan(config.tracer(serviceName), name, extracted.context,
                SpanKind.CONSUMER, destination, "receive");
        span.setAttribute(AttributeKey.stringKey("trace.context.source"), extracted.source);
        if (message.getJMSMessageID() != null) {
            span.setAttribute(AttributeKey.stringKey("messaging.message.id"), message.getJMSMessageID());
        }
        Map<String, String> context = Tracing.inject(Context.root().with(span));
        span.end();
        return Messages.attributes(message, destination, context, extracted.source);
    }

    private static void applyHeaders(Message message, Session session, Map<String, String> userProperties,
                                     String correlationId, String replyTo, DestinationType replyToType,
                                     boolean dmqEligible) throws JMSException {
        for (Map.Entry<String, String> property : userProperties.entrySet()) {
            message.setStringProperty(property.getKey(), property.getValue());
        }
        if (!blank(correlationId)) {
            message.setJMSCorrelationID(correlationId);
        }
        if (!blank(replyTo)) {
            message.setJMSReplyTo(Messages.destination(session, replyToType, replyTo));
        }
        if (dmqEligible) {
            message.setBooleanProperty("JMS_Solace_DeadMsgQueueEligible", true);
        }
    }

    private static int deliveryMode(boolean persistent) {
        return persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT;
    }

    private static boolean blank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private static void fail(Span span, Exception e) {
        span.recordException(e);
        span.setStatus(StatusCode.ERROR, e.getMessage() == null ? "" : e.getMessage());
    }

    private static ModuleException jmsFailure(String what, JMSException e, SolaceConnection connection, SolaceError type) {
        if (!connection.isValid() || e instanceof javax.jms.IllegalStateException) {
            connection.invalidate();   // Mule will validate, discard and reconnect
            return new ModuleException("Lost connection to Solace while " + what,
                    SolaceError.CONNECTIVITY, new ConnectionException(e.getMessage(), e));
        }
        return new ModuleException("Failed " + what + ": " + e.getMessage(), type, e);
    }

    private static void closeQuietly(MessageConsumer consumer, Session session) {
        if (consumer != null) {
            try {
                consumer.close();
            } catch (JMSException e) {
                LOGGER.debug("Error closing consumer", e);
            }
        }
        if (session != null) {
            try {
                session.close();
            } catch (JMSException e) {
                LOGGER.debug("Error closing session", e);
            }
        }
    }

    /** Holds a Consume (MANUAL) session open until Ack or Recover session. */
    private static final class ConsumeAckHandle implements AckHandle {
        private final Session session;
        private final MessageConsumer consumer;
        private final Message message;

        ConsumeAckHandle(Session session, MessageConsumer consumer, Message message) {
            this.session = session;
            this.consumer = consumer;
            this.message = message;
        }

        @Override
        public void ack() throws JMSException {
            try {
                message.acknowledge();
            } finally {
                closeQuietly(consumer, session);
            }
        }

        @Override
        public void recover() throws JMSException {
            try {
                session.recover();
            } finally {
                closeQuietly(consumer, session);
            }
        }

        @Override
        public void expire() {
            // Closing an unacknowledged CLIENT_ACKNOWLEDGE session returns the message to the queue.
            closeQuietly(consumer, session);
        }
    }
}
