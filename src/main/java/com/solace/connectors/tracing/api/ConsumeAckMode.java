package com.solace.connectors.tracing.api;

/** How the Consume operation acknowledges the message it receives. */
public enum ConsumeAckMode {
    /** Ack as soon as the message is received. */
    AUTO,
    /** Keep the message unacknowledged until the Ack or Recover session operation is called with attributes.ackId. */
    MANUAL
}
