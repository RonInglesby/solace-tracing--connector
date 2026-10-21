package com.solace.connectors.tracing.internal.source;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.BytesMessage;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TextMessage;

import com.solace.connectors.tracing.api.SolaceMessageAttributes;
import com.solace.connectors.tracing.internal.SolaceConnection;
import com.solace.connectors.tracing.internal.SolaceTracingConfiguration;
import com.solace.connectors.tracing.internal.Tracing;
import com.solace.opentelemetry.javaagent.jms.SolaceJmsW3CTextMapGetter;

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
import org.mule.runtime.extension.api.annotation.Alias;
import org.mule.runtime.extension.api.annotation.execution.OnError;
import org.mule.runtime.extension.api.annotation.execution.OnSuccess;
import org.mule.runtime.extension.api.annotation.execution.OnTerminate;
import org.mule.runtime.extension.api.annotation.param.Config;
import org.mule.runtime.extension.api.annotation.param.Connection;
import org.mule.runtime.extension.api.annotation.param.Optional;
import org.mule.runtime.extension.api.annotation.param.Parameter;
import org.mule.runtime.extension.api.annotation.param.display.DisplayName;
import org.mule.runtime.extension.api.annotation.param.display.Summary;
import org.mule.runtime.extension.api.runtime.operation.Result;
import org.mule.runtime.extension.api.runtime.source.Source;
import org.mule.runtime.extension.api.runtime.source.SourceCallback;
import org.mule.runtime.extension.api.runtime.source.SourceCallbackContext;
import org.mule.runtime.extension.api.runtime.source.SourceResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Push-based queue listener. For each message it:
 *  1. reads the trace context the broker wrote onto the message (SolaceJmsW3CTextMapGetter), falling back to
 *     the traceparent user property;
 *  2. starts a CONSUMER span under it and runs the flow;
 *  3. acknowledges the message if the flow succeeds, or calls recover() so the broker redelivers it.
 *
 * Each consumer has its own connection and session. The Solace dispatcher thread waits for the flow to finish
 * before acking, so ack/recover always run on the thread that owns the JMS session, and each consumer has at
 * most one message in flight. Use numberOfConsumers for concurrency (needs a non-exclusive queue).
 */
@Alias("listener")
@DisplayName("On New Message")
@org.mule.runtime.extension.api.annotation.param.MediaType(
        value = org.mule.runtime.extension.api.annotation.param.MediaType.ANY, strict = false)
public class SolaceQueueListener extends Source<String, SolaceMessageAttributes> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SolaceQueueListener.class);
    private static final String LATCH = "solace.ack.latch";

    @Config
    private SolaceTracingConfiguration config;

    @Connection
    private ConnectionProvider<SolaceConnection> connectionProvider;

    @Parameter
    @DisplayName("Queue name")
    private String queueName;

    @Parameter
    @Optional(defaultValue = "1")
    @Summary("Concurrent consumers, each with its own connection. More than 1 needs a non-exclusive queue.")
    private int numberOfConsumers;

    @Parameter
    @Optional(defaultValue = "application/json")
    @Summary("Media type given to the message body")
    private String contentType;

    @Parameter
    @Optional
    @Summary("Receive span name (default: '<queue> receive')")
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
        try {
            for (int i = 0; i < consumers; i++) {
                SolaceConnection connection = connectionProvider.connect();
                Worker worker = new Worker(connection);
                workers.add(worker);

                final Session session = connection.createSession(Session.CLIENT_ACKNOWLEDGE);
                worker.session = session;
                worker.consumer = session.createConsumer(session.createQueue(queueName));
                worker.consumer.setMessageListener(new MessageListener() {
                    @Override
                    public void onMessage(Message message) {
                        handle(message, session, sourceCallback);
                    }
                });
                connection.addExceptionListener(new ExceptionListener() {
                    @Override
                    public void onException(JMSException e) {
                        // Solace API reconnect retries are exhausted: let Mule restart the source.
                        if (!stopping && connectionFailureReported.compareAndSet(false, true)) {
                            sourceCallback.onConnectionException(new ConnectionException(
                                    "Lost Solace connection for queue " + queueName + ": " + e.getMessage(), e));
                        }
                    }
                });
                connection.start();
            }
            LOGGER.info("Solace listener started on queue '{}' with {} consumer(s)", queueName, consumers);
        } catch (ConnectionException e) {
            closeWorkers();
            throw e;
        } catch (JMSException e) {
            closeWorkers();
            throw new ConnectionException("Could not start Solace listener on queue " + queueName + ": "
                    + e.getMessage(), e);
        }
    }

    @Override
    public void onStop() {
        stopping = true;
        // Give in-flight flows a chance to finish so their messages are acked, not redelivered.
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
        LOGGER.info("Solace listener stopped on queue '{}'", queueName);
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
    private void handle(Message message, Session session, SourceCallback<String, SolaceMessageAttributes> callback) {
        if (stopping) {
            recover(session);
            return;
        }

        // Prefer the broker-updated Solace-native context; fall back to our traceparent user property.
        Context parent = Tracing.PROPAGATOR.extract(Context.root(), message, new SolaceJmsW3CTextMapGetter());
        String source = "solace";
        if (!Span.fromContext(parent).getSpanContext().isValid()) {
            parent = Tracing.extract(stringProperty(message, "traceparent"), stringProperty(message, "tracestate"));
            source = Span.fromContext(parent).getSpanContext().isValid() ? "user-property" : "none";
        }

        Span span = config.tracer(serviceName)
                .spanBuilder(spanName == null || spanName.isEmpty() ? queueName + " receive" : spanName)
                .setParent(parent)
                .setSpanKind(SpanKind.CONSUMER)
                .setAttribute(AttributeKey.stringKey("messaging.system"), "solace")
                .setAttribute(AttributeKey.stringKey("messaging.operation"), "receive")
                .setAttribute(AttributeKey.stringKey("messaging.destination.name"), queueName)
                .setAttribute(AttributeKey.stringKey("trace.context.source"), source)
                .startSpan();

        AckLatch latch = new AckLatch();
        inFlight.add(latch);
        boolean success = false;
        try (Scope scope = span.makeCurrent()) {
            Map<String, String> context = Tracing.inject(Context.current());
            SolaceMessageAttributes attributes = attributes(message, context, source);
            if (attributes.getMessageId() != null) {
                span.setAttribute(AttributeKey.stringKey("messaging.message.id"), attributes.getMessageId());
            }

            Result<String, SolaceMessageAttributes> result = Result.<String, SolaceMessageAttributes>builder()
                    .output(body(message))
                    .mediaType(MediaType.parse(contentType))
                    .attributes(attributes)
                    .build();

            SourceCallbackContext callbackContext = callback.createContext();
            callbackContext.addVariable(LATCH, latch);
            callback.handle(result, callbackContext);

            success = latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            span.recordException(e);
        } catch (Exception e) {
            LOGGER.error("Could not dispatch message from queue '{}'", queueName, e);
            span.recordException(e);
        } finally {
            inFlight.remove(latch);
            if (success) {
                try {
                    message.acknowledge();
                } catch (JMSException e) {
                    LOGGER.warn("Could not acknowledge message from queue '{}'; it may be redelivered", queueName, e);
                }
            } else {
                span.setStatus(StatusCode.ERROR, "Flow failed; message recovered for redelivery");
                recover(session);
            }
            span.end();
        }
    }

    @OnSuccess
    public void onSuccess(SourceCallbackContext callbackContext) {
        latch(callbackContext, true);
    }

    @OnError
    public void onError(SourceCallbackContext callbackContext) {
        latch(callbackContext, false);
    }

    /** Safety net: releases the dispatcher thread if neither OnSuccess nor OnError ran (e.g. backpressure). */
    @OnTerminate
    public void onTerminate(SourceResult sourceResult) {
        latch(sourceResult.getSourceCallbackContext(), sourceResult.isSuccess());
    }

    private void latch(SourceCallbackContext callbackContext, boolean success) {
        java.util.Optional<AckLatch> latch = callbackContext.getVariable(LATCH);
        if (latch.isPresent()) {
            latch.get().release(success);
        }
    }

    private void recover(Session session) {
        try {
            session.recover();
        } catch (JMSException e) {
            LOGGER.warn("Could not recover session for queue '{}'", queueName, e);
        }
    }

    private SolaceMessageAttributes attributes(Message message, Map<String, String> context, String source)
            throws JMSException {
        SolaceMessageAttributes attributes = new SolaceMessageAttributes();
        attributes.setMessageId(message.getJMSMessageID());
        attributes.setCorrelationId(message.getJMSCorrelationID());
        attributes.setQueueName(queueName);
        attributes.setDestination(message.getJMSDestination() == null ? null : message.getJMSDestination().toString());
        attributes.setRedelivered(message.getJMSRedelivered());
        attributes.setTimestamp(message.getJMSTimestamp());
        attributes.setTraceparent(context.get("traceparent"));
        attributes.setTracestate(context.get("tracestate"));
        attributes.setTraceContextSource(source);

        Map<String, String> properties = new HashMap<String, String>();
        Enumeration<?> names = message.getPropertyNames();
        while (names != null && names.hasMoreElements()) {
            String name = String.valueOf(names.nextElement());
            Object value = message.getObjectProperty(name);
            properties.put(name, value == null ? null : String.valueOf(value));
        }
        attributes.setProperties(properties);
        return attributes;
    }

    private static String body(Message message) throws JMSException {
        if (message instanceof TextMessage) {
            String text = ((TextMessage) message).getText();
            return text == null ? "" : text;
        }
        if (message instanceof BytesMessage) {
            BytesMessage bytesMessage = (BytesMessage) message;
            byte[] bytes = new byte[(int) bytesMessage.getBodyLength()];
            bytesMessage.readBytes(bytes);
            return new String(bytes, StandardCharsets.UTF_8);
        }
        return "";
    }

    private static String stringProperty(Message message, String name) {
        try {
            return message.getStringProperty(name);
        } catch (JMSException e) {
            return null;
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
}
