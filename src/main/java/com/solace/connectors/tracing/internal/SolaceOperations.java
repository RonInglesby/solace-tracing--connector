package com.solace.connectors.tracing.internal;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

import com.solace.connectors.tracing.api.DestinationType;
import com.solace.connectors.tracing.internal.SolaceConnection.PublisherSession;
import com.solace.opentelemetry.javaagent.jms.SolaceJmsW3CTextMapSetter;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;

import org.mule.runtime.api.connection.ConnectionException;
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

public class SolaceOperations {

    /**
     * Publishes a text message and puts the trace context on it twice: in the Solace-native context
     * (read and updated by the broker) and as traceparent/tracestate user properties (fallback).
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
                                       @Optional(defaultValue = "true") boolean persistent,
                                       @Optional(defaultValue = "true") @DisplayName("DMQ eligible") boolean dmqEligible,
                                       @Optional @NullSafe Map<String, String> userProperties) {

        Span span = config.tracer(serviceName)
                .spanBuilder(spanName == null || spanName.isEmpty() ? destination + " send" : spanName)
                .setParent(Tracing.extract(traceparent, tracestate))
                .setSpanKind(SpanKind.PRODUCER)
                .setAttribute(AttributeKey.stringKey("messaging.system"), "solace")
                .setAttribute(AttributeKey.stringKey("messaging.operation"), "send")
                .setAttribute(AttributeKey.stringKey("messaging.destination.name"), destination)
                .setAttribute(AttributeKey.stringKey("messaging.destination.kind"), destinationType.name().toLowerCase())
                .startSpan();

        PublisherSession publisher = null;
        boolean broken = false;
        try (Scope scope = span.makeCurrent()) {
            publisher = connection.borrowPublisher();
            Destination dest = destinationType == DestinationType.QUEUE
                    ? publisher.session.createQueue(destination)
                    : publisher.session.createTopic(destination);

            TextMessage message = publisher.session.createTextMessage(text(body));
            for (Map.Entry<String, String> property : userProperties.entrySet()) {
                message.setStringProperty(property.getKey(), property.getValue());
            }
            if (dmqEligible) {
                message.setBooleanProperty("JMS_Solace_DeadMsgQueueEligible", true);
            }

            // 1) traceparent/tracestate user properties (fallback for consumers without the Solace getter)
            Map<String, String> out = Tracing.inject(Context.current());
            for (Map.Entry<String, String> entry : out.entrySet()) {
                message.setStringProperty(entry.getKey(), entry.getValue());
            }
            // 2) Solace-native context: the broker adds its spans and hands its own context to the consumer
            Tracing.PROPAGATOR.inject(Context.current(), message, new SolaceJmsW3CTextMapSetter());

            publisher.producer.send(dest, message,
                    persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT,
                    Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);

            String messageId = message.getJMSMessageID();
            out.put("messageId", messageId == null ? "" : messageId);
            if (messageId != null) {
                span.setAttribute(AttributeKey.stringKey("messaging.message.id"), messageId);
            }
            return out;
        } catch (IOException e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            throw new ModuleException("Could not read the message body for " + destination + ": " + e.getMessage(),
                    SolaceError.PUBLISH, e);
        } catch (JMSException e) {
            broken = true;
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            if (!connection.isValid() || e instanceof javax.jms.IllegalStateException) {
                connection.invalidate();   // Mule will validate, discard and reconnect
                throw new ModuleException("Lost connection to Solace while publishing to " + destination,
                        SolaceError.CONNECTIVITY, new ConnectionException(e.getMessage(), e));
            }
            throw new ModuleException("Failed to publish to " + destination + ": " + e.getMessage(),
                    SolaceError.PUBLISH, e);
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

    /** Reads the body stream as text, using its declared charset (UTF-8 if none). No DataWeave conversion. */
    private static String text(TypedValue<InputStream> body) throws IOException {
        if (body == null || body.getValue() == null) {
            return "";
        }
        Charset charset = StandardCharsets.UTF_8;
        if (body.getDataType() != null && body.getDataType().getMediaType() != null) {
            charset = body.getDataType().getMediaType().getCharset().orElse(StandardCharsets.UTF_8);
        }
        InputStream in = body.getValue();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buffer = new byte[8192];
        int read;
        while ((read = in.read(buffer)) != -1) {
            out.write(buffer, 0, read);
        }
        return new String(out.toByteArray(), charset);
    }
}
