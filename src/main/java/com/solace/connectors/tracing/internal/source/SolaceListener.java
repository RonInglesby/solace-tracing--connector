package com.solace.connectors.tracing.internal.source;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TextMessage;

import com.solace.connectors.tracing.api.AckMode;
import com.solace.connectors.tracing.api.DestinationType;
import com.solace.connectors.tracing.api.SolaceMessageAttributes;
import com.solace.connectors.tracing.internal.Messages;
import com.solace.connectors.tracing.internal.SolaceConnection;
import com.solace.connectors.tracing.internal.SolaceConnection.PublisherSession;
import com.solace.connectors.tracing.internal.SolaceTracingConfiguration;
import com.solace.connectors.tracing.internal.Tracing;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;

import org.mule.runtime.api.connection.ConnectionException;
import org.mule.runtime.api.connection.ConnectionProvider;
import org.mule.runtime.api.exception.MuleException;
import org.mule.runtime.api.metadata.MediaType;
import org.mule.runtime.api.metadata.TypedValue;
import org.mule.runtime.extension.api.annotation.Alias;
import org.mule.runtime.extension.api.annotation.execution.OnError;
import org.mule.runtime.extension.api.annotation.execution.OnSuccess;
import org.mule.runtime.extension.api.annotation.execution.OnTerminate;
import org.mule.runtime.extension.api.annotation.param.Config;
import org.mule.runtime.extension.api.annotation.param.Connection;
import org.mule.runtime.extension.api.annotation.param.Content;
import org.mule.runtime.extension.api.annotation.param.Optional;
import org.mule.runtime.extension.api.annotation.param.Parameter;
import org.mule.runtime.extension.api.annotation.param.display.DisplayName;
import org.mule.runtime.extension.api.annotation.param.display.Summary;
import org.mule.runtime.extension.api.annotation.source.EmitsResponse;
import org.mule.runtime.extension.api.runtime.operation.Result;
import org.mule.runtime.extension.api.runtime.source.Source;
import org.mule.runtime.extension.api.runtime.source.SourceCallback;
import org.mule.runtime.extension.api.runtime.source.SourceCallbackContext;
import org.mule.runtime.extension.api.runtime.source.SourceResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Push-based listener for a queue or topic. For each message it:
 *  1. reads the trace context the broker wrote onto the message, falling back to the traceparent user property;
 *  2. starts a CONSUMER span under it and runs the flow;
 *  3. acknowledges per the ack mode (AUTO: ack on success / redeliver on failure; MANUAL: the flow decides with
 *     the Ack operation; IMMEDIATE: ack before the flow);
 *  4. if the message has a JMSReplyTo, sends the flow's result back as the reply, traced as a child of the
 *     receive span, with JMSCorrelationID copied from the request.
 *
 * Each consumer has its own connection and session. The Solace dispatcher thread waits for the flow to finish
 * before acking, so ack/recover always run on the thread that owns the JMS session, and each consumer has at most
 * one message in flight. Use numberOfConsumers for concurrency on a non-exclusive queue.
 */
@Alias("listener")
@DisplayName("On New Message")
@EmitsResponse
@org.mule.runtime.extension.api.annotation.param.MediaType(
        value = org.mule.runtime.extension.api.annotation.param.MediaType.ANY, strict = false)
public class SolaceListener extends Source<String, SolaceMessageAttributes> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SolaceListener.class);
    private static final String LATCH = "solace.ack.latch";
    private static final String REPLY = "solace.reply";

    @Config
    private SolaceTracingConfiguration config;

    @Connection
    private ConnectionProvider<SolaceConnection> connectionProvider;

    @Parameter
    @Summary("Queue name, or topic (wildcards allowed) when consumerType is TOPIC")
    private String destination;

    @Parameter
    @Optional(defaultValue = "QUEUE")
    @Summary("QUEUE, or TOPIC for a non-durable topic subscription")
    private DestinationType consumerType;

    @Parameter
    @Optional(defaultValue = "AUTO")
    private AckMode ackMode;

    @Parameter
    @Optional
    @Summary("JMS message selector")
    private String selector;

    @Parameter
    @Optional(defaultValue = "1")
    @Summary("Concurrent consumers, each with its own connection. Queues only; needs a non-exclusive queue.")
    private int numberOfConsumers;

    @Parameter
    @Optional(defaultValue = "application/json")
    @Summary("Media type given to the message body")
    private String contentType;

    @Parameter
    @Optional(defaultValue = "true")
    @Summary("Send the flow result back when a message has JMSReplyTo")
    private boolean sendReplies;

    @Parameter
    @Optional
    @Summary("Receive span name (default: '<destination> receive')")
    private String spanName;

    @Parameter
    @Optional
    @Summary("Overrides the configuration's default service name")
    private String serviceName;

    @Parameter
    @Optional(defaultValue = "30000")
    @DisplayName("Shutdown timeout (ms)")
    @Summary("How long stop waits for in-flight messages before recovering them for redelivery")
    private long shutdownTimeoutMs;

    private final List<Worker> workers = new ArrayList<Worker>();
    private final Set<AckLatch> inFlight = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean connectionFailureReported = new AtomicBoolean(false);
    private volatile boolean stopping;

    @Override
    public void onStart(final SourceCallback<String, SolaceMessageAttributes> sourceCallback) throws MuleException {
        stopping = false;
        connectionFailureReported.set(false);
        int consumers = Math.max(1, numberOfConsumers);
        if (consumerType == DestinationType.TOPIC && consumers > 1) {
            LOGGER.warn("Topic listener on '{}': numberOfConsumers ignored (each topic subscriber gets every message). "
                    + "For parallel topic processing use a non-exclusive queue subscribed to the topic.", destination);
            consumers = 1;
        }
        try {
            for (int i = 0; i < consumers; i++) {
                SolaceConnection connection = connectionProvider.connect();
                final Worker worker = new Worker(connection);
                workers.add(worker);

                worker.session = connection.createSession(Session.CLIENT_ACKNOWLEDGE);
                Destination dest = Messages.destination(worker.session, consumerType, destination);
                worker.consumer = (selector == null || selector.trim().isEmpty())
                        ? worker.session.createConsumer(dest)
                        : worker.session.createConsumer(dest, selector);
                worker.consumer.setMessageListener(new MessageListener() {
                    @Override
                    public void onMessage(Message message) {
                        handle(message, worker, sourceCallback);
                    }
                });
                connection.addExceptionListener(new ExceptionListener() {
                    @Override
                    public void onException(JMSException e) {
                        // Solace API reconnect retries are exhausted: let Mule restart the source.
                        if (!stopping && connectionFailureReported.compareAndSet(false, true)) {
                            sourceCallback.onConnectionException(new ConnectionException(
                                    "Lost Solace connection for " + destination + ": " + e.getMessage(), e));
                        }
                    }
                });
                connection.start();
            }
            LOGGER.info("Solace listener started on {} '{}' ({} consumer(s), ack mode {})",
                    consumerType, destination, consumers, ackMode);
        } catch (ConnectionException e) {
            closeWorkers();
            throw e;
        } catch (JMSException e) {
            closeWorkers();
            throw new ConnectionException("Could not start Solace listener on " + destination + ": " + e.getMessage(), e);
        }
    }

    @Override
    public void onStop() {
        stopping = true;
        long deadline = System.currentTimeMillis() + Math.max(0L, shutdownTimeoutMs);
        while (!inFlight.isEmpty() && System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        for (AckLatch latch : inFlight) {
            latch.release(false);   // still running: recover, the broker will redeliver
        }
        closeWorkers();
        LOGGER.info("Solace listener stopped on '{}'", destination);
    }

    private void closeWorkers() {
        for (Worker worker : workers) {
            if (worker.consumer != null) {
                try {
                    worker.consumer.close();
                } catch (JMSException e) {
                    LOGGER.debug("Error closing consumer", e);
                }
            }
            if (worker.session != null) {
                try {
                    worker.session.close();
                } catch (JMSException e) {
                    LOGGER.debug("Error closing session", e);
                }
            }
            try {
                connectionProvider.disconnect(worker.connection);
            } catch (RuntimeException e) {
                LOGGER.debug("Error disconnecting", e);
            }
        }
        workers.clear();
    }

    /** Runs on the Solace dispatcher thread of this consumer's connection. */
    private void handle(Message message, Worker worker, SourceCallback<String, SolaceMessageAttributes> callback) {
        Session session = worker.session;
        if (stopping) {
            recover(session);
            return;
        }

        Tracing.Extracted extracted = Tracing.extract(message);
        Span span = Tracing.startSpan(config.tracer(serviceName),
                spanName == null || spanName.isEmpty() ? destination + " receive" : spanName,
                extracted.context, SpanKind.CONSUMER, destination, "receive");
        span.setAttribute(AttributeKey.stringKey("trace.context.source"), extracted.source);

        AckLatch latch = new AckLatch();
        ListenerAckHandle manualHandle = null;
        String ackId = null;
        inFlight.add(latch);
        boolean success = false;
        try (Scope scope = span.makeCurrent()) {
            if (ackMode == AckMode.IMMEDIATE) {
                message.acknowledge();
            } else if (ackMode == AckMode.MANUAL) {
                manualHandle = new ListenerAckHandle();
                ackId = config.acks().register(manualHandle);
            }

            Map<String, String> context = Tracing.inject(Context.current());
            SolaceMessageAttributes attributes = Messages.attributes(message, destination, context, extracted.source);
            attributes.setAckId(ackId);
            if (attributes.getMessageId() != null) {
                span.setAttribute(AttributeKey.stringKey("messaging.message.id"), attributes.getMessageId());
            }

            Result<String, SolaceMessageAttributes> result = Result.<String, SolaceMessageAttributes>builder()
                    .output(Messages.body(message))
                    .mediaType(MediaType.parse(contentType))
                    .attributes(attributes)
                    .build();

            SourceCallbackContext callbackContext = callback.createContext();
            callbackContext.addVariable(LATCH, latch);
            if (sendReplies && message.getJMSReplyTo() != null) {
                callbackContext.addVariable(REPLY, new ReplyInfo(message.getJMSReplyTo(),
                        Messages.replyCorrelationId(message), Context.current(), worker.connection));
            }
            callback.handle(result, callbackContext);

            success = latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            span.recordException(e);
        } catch (Exception e) {
            LOGGER.error("Could not dispatch message from '{}'", destination, e);
            span.recordException(e);
        } finally {
            inFlight.remove(latch);
            if (ackId != null) {
                config.acks().remove(ackId);   // no-op if the Ack operation already took it
            }
            settle(message, session, success, manualHandle);
            if (!success) {
                span.setStatus(StatusCode.ERROR, "Flow failed");
            }
            span.end();
        }
    }

    /** Applies the ack decision on the dispatcher thread that owns the session. */
    private void settle(Message message, Session session, boolean success, ListenerAckHandle manualHandle) {
        boolean acknowledge;
        switch (ackMode) {
            case IMMEDIATE:
                return;   // already acknowledged on receipt
            case MANUAL:
                acknowledge = manualHandle != null && manualHandle.decision() == ListenerAckHandle.Decision.ACK;
                break;
            case AUTO:
            default:
                acknowledge = success;
                break;
        }
        if (acknowledge) {
            try {
                message.acknowledge();
            } catch (JMSException e) {
                LOGGER.warn("Could not acknowledge message from '{}'; it may be redelivered", destination, e);
            }
        } else {
            recover(session);
        }
    }

    /** Sends the reply (if the request asked for one), then releases the dispatcher thread. */
    @OnSuccess
    public void onSuccess(@Optional(defaultValue = "#[payload]") @Content @DisplayName("Reply body")
                          TypedValue<InputStream> replyBody,
                          SourceCallbackContext callbackContext) {
        boolean ok = true;
        java.util.Optional<ReplyInfo> reply = callbackContext.getVariable(REPLY);
        if (reply.isPresent()) {
            try {
                sendReply(reply.get(), replyBody);
            } catch (Exception e) {
                LOGGER.error("Could not send reply for message from '{}'; it will be redelivered", destination, e);
                ok = false;
            }
        }
        release(callbackContext, ok);
    }

    @OnError
    public void onError(SourceCallbackContext callbackContext) {
        release(callbackContext, false);
    }

    /** Safety net: releases the dispatcher thread if neither OnSuccess nor OnError ran (e.g. backpressure). */
    @OnTerminate
    public void onTerminate(SourceResult sourceResult) {
        release(sourceResult.getSourceCallbackContext(), sourceResult.isSuccess());
    }

    private void release(SourceCallbackContext callbackContext, boolean success) {
        java.util.Optional<AckLatch> latch = callbackContext.getVariable(LATCH);
        if (latch.isPresent()) {
            latch.get().release(success);
        }
    }

    private void sendReply(ReplyInfo info, TypedValue<InputStream> replyBody) throws Exception {
        String replyName = Messages.name(info.replyTo);
        Span span = Tracing.startSpan(config.tracer(serviceName), (replyName == null ? "reply" : replyName) + " send",
                info.receiveContext, SpanKind.PRODUCER, replyName, "send");
        PublisherSession publisher = null;
        boolean broken = false;
        try (Scope scope = span.makeCurrent()) {
            publisher = info.connection.borrowPublisher();
            TextMessage reply = publisher.session.createTextMessage(Messages.text(replyBody));
            if (info.correlationId != null) {
                reply.setJMSCorrelationID(info.correlationId);
            }
            Tracing.inject(Context.current(), reply);
            publisher.producer.send(info.replyTo, reply, DeliveryMode.PERSISTENT,
                    Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
        } catch (Exception e) {
            broken = e instanceof JMSException;
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage() == null ? "" : e.getMessage());
            throw e;
        } finally {
            if (publisher != null) {
                if (broken) {
                    info.connection.discardPublisher(publisher);
                } else {
                    info.connection.returnPublisher(publisher);
                }
            }
            span.end();
        }
    }

    private void recover(Session session) {
        try {
            session.recover();
        } catch (JMSException e) {
            LOGGER.warn("Could not recover session for '{}'", destination, e);
        }
    }

    private static final class Worker {
        final SolaceConnection connection;
        Session session;
        MessageConsumer consumer;

        Worker(SolaceConnection connection) {
            this.connection = connection;
        }
    }

    /** What OnSuccess needs to reply: where to, correlation, the receive span's context and a connection. */
    private static final class ReplyInfo {
        final Destination replyTo;
        final String correlationId;
        final Context receiveContext;
        final SolaceConnection connection;

        ReplyInfo(Destination replyTo, String correlationId, Context receiveContext, SolaceConnection connection) {
            this.replyTo = replyTo;
            this.correlationId = correlationId;
            this.receiveContext = receiveContext;
            this.connection = connection;
        }
    }
}
