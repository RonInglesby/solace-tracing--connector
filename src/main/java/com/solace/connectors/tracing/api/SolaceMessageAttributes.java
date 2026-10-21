package com.solace.connectors.tracing.api;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Attributes of a message received by the listener.
 * traceparent/tracestate identify the listener's receive span: pass them to the publish operation
 * so the next hop becomes its child.
 */
public class SolaceMessageAttributes implements Serializable {

    private static final long serialVersionUID = 1L;

    private String messageId;
    private String correlationId;
    private String queueName;
    private String destination;
    private boolean redelivered;
    private long timestamp;
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

    public String getQueueName() { return queueName; }
    public void setQueueName(String queueName) { this.queueName = queueName; }

    public String getDestination() { return destination; }
    public void setDestination(String destination) { this.destination = destination; }

    public boolean isRedelivered() { return redelivered; }
    public void setRedelivered(boolean redelivered) { this.redelivered = redelivered; }

    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

    /** traceparent of the listener's receive span. */
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
        return "SolaceMessageAttributes{messageId=" + messageId + ", queueName=" + queueName
                + ", destination=" + destination + ", redelivered=" + redelivered
                + ", traceparent=" + traceparent + ", traceContextSource=" + traceContextSource + "}";
    }
}
