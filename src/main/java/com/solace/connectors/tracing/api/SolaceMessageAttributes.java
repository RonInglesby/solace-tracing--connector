package com.solace.connectors.tracing.api;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Attributes of a received message (listener, Consume and Publish consume).
 * traceparent/tracestate identify the receive span: pass them to Publish so the next hop becomes its child.
 */
public class SolaceMessageAttributes implements Serializable {

    private static final long serialVersionUID = 2L;

    private String messageId;
    private String correlationId;
    private String destination;
    private String replyTo;
    private boolean redelivered;
    private long timestamp;
    private int priority;
    private String ackId;
    private String traceparent;
    private String tracestate;
    private String traceContextSource;
    private Map<String, String> properties = new HashMap<String, String>();

    public SolaceMessageAttributes() {
    }

    public String getMessageId() { return messageId; }
    public void setMessageId(String messageId) { this.messageId = messageId; }

    public String getCorrelationId() { return correlationId; }
    public void setCorrelationId(String correlationId) { this.correlationId = correlationId; }

    /** Queue or topic the message was received from. */
    public String getDestination() { return destination; }
    public void setDestination(String destination) { this.destination = destination; }

    /** JMSReplyTo of the message, if any. */
    public String getReplyTo() { return replyTo; }
    public void setReplyTo(String replyTo) { this.replyTo = replyTo; }

    public boolean isRedelivered() { return redelivered; }
    public void setRedelivered(boolean redelivered) { this.redelivered = redelivered; }

    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

    public int getPriority() { return priority; }
    public void setPriority(int priority) { this.priority = priority; }

    /** Set in MANUAL ack mode: pass to the Ack or Recover session operation. */
    public String getAckId() { return ackId; }
    public void setAckId(String ackId) { this.ackId = ackId; }

    /** traceparent of the receive span. */
    public String getTraceparent() { return traceparent; }
    public void setTraceparent(String traceparent) { this.traceparent = traceparent; }

    public String getTracestate() { return tracestate; }
    public void setTracestate(String tracestate) { this.tracestate = tracestate; }

    /** "solace" (broker-updated context), "user-property" (fallback) or "none" (new trace). */
    public String getTraceContextSource() { return traceContextSource; }
    public void setTraceContextSource(String traceContextSource) { this.traceContextSource = traceContextSource; }

    /** JMS message properties, as strings. */
    public Map<String, String> getProperties() { return properties; }
    public void setProperties(Map<String, String> properties) { this.properties = properties; }

    @Override
    public String toString() {
        return "SolaceMessageAttributes{messageId=" + messageId + ", destination=" + destination
                + ", correlationId=" + correlationId + ", redelivered=" + redelivered + ", ackId=" + ackId
                + ", traceparent=" + traceparent + ", traceContextSource=" + traceContextSource + "}";
    }
}
