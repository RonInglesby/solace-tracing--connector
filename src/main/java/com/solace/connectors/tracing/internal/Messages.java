package com.solace.connectors.tracing.internal;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import com.solace.connectors.tracing.api.DestinationType;
import com.solace.connectors.tracing.api.SolaceMessageAttributes;

import org.mule.runtime.api.metadata.TypedValue;

/** Message helpers shared by the listener and the operations. */
public final class Messages {

    private Messages() {
    }

    public static Destination destination(Session session, DestinationType type, String name) throws JMSException {
        return type == DestinationType.QUEUE ? session.createQueue(name) : session.createTopic(name);
    }

    /** Waits for a message: -1 = forever, 0 = don't wait, otherwise milliseconds. Null when nothing arrived. */
    public static Message receive(MessageConsumer consumer, long maximumWait) throws JMSException {
        if (maximumWait < 0) {
            return consumer.receive();
        }
        if (maximumWait == 0) {
            return consumer.receiveNoWait();
        }
        return consumer.receive(maximumWait);
    }

    /** Body as text (TextMessage text, or BytesMessage bytes as UTF-8). */
    public static String body(Message message) throws JMSException {
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

    /** Reads a Mule content stream as text using its declared charset (UTF-8 if none). No DataWeave conversion. */
    public static String text(TypedValue<InputStream> body) throws IOException {
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

    /** Readable name of a JMS destination. */
    public static String name(Destination destination) {
        if (destination == null) {
            return null;
        }
        try {
            if (destination instanceof Queue) {
                return ((Queue) destination).getQueueName();
            }
            if (destination instanceof Topic) {
                return ((Topic) destination).getTopicName();
            }
        } catch (JMSException e) {
            // fall through
        }
        return destination.toString();
    }

    /** Correlation id to put on a reply: the request's correlation id, else its message id. */
    public static String replyCorrelationId(Message request) throws JMSException {
        String correlationId = request.getJMSCorrelationID();
        return (correlationId == null || correlationId.isEmpty()) ? request.getJMSMessageID() : correlationId;
    }

    public static SolaceMessageAttributes attributes(Message message, String destinationName,
                                                     Map<String, String> traceContext, String traceContextSource)
            throws JMSException {
        SolaceMessageAttributes attributes = new SolaceMessageAttributes();
        attributes.setMessageId(message.getJMSMessageID());
        attributes.setCorrelationId(message.getJMSCorrelationID());
        String received = name(message.getJMSDestination());
        attributes.setDestination(received != null ? received : destinationName);
        attributes.setReplyTo(name(message.getJMSReplyTo()));
        attributes.setRedelivered(message.getJMSRedelivered());
        attributes.setTimestamp(message.getJMSTimestamp());
        attributes.setPriority(message.getJMSPriority());
        attributes.setTraceparent(traceContext.get("traceparent"));
        attributes.setTracestate(traceContext.get("tracestate"));
        attributes.setTraceContextSource(traceContextSource);

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
}
