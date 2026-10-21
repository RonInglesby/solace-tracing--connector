package com.solace.connectors.tracing.internal;

import javax.jms.JMSException;

/** A message waiting for a manual Ack or Recover session. */
public interface AckHandle {

    /** Acknowledge the message. */
    void ack() throws JMSException;

    /** Hand the message back to the broker for redelivery. */
    void recover() throws JMSException;

    /** Called when the handle is dropped without a decision (timeout or app stop): must release resources. */
    void expire();
}
