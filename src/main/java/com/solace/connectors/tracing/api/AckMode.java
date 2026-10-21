package com.solace.connectors.tracing.api;

/** How the listener acknowledges messages. */
public enum AckMode {
    /** Ack when the flow succeeds; recover (redeliver) when it fails. At-least-once. */
    AUTO,
    /** The flow acks with the Ack operation (attributes.ackId). Applied when the flow finishes; no ack means redelivery. */
    MANUAL,
    /** Ack on receipt, before the flow runs. At-most-once: a failed flow loses the message. */
    IMMEDIATE
}
