package com.solace.connectors.tracing.internal.source;

import com.solace.connectors.tracing.internal.AckHandle;

/**
 * MANUAL-mode decision for a listener message. The Ack / Recover session operations only record the decision;
 * the listener applies it when the flow finishes, on the thread that owns the JMS session.
 */
final class ListenerAckHandle implements AckHandle {

    enum Decision { NONE, ACK, RECOVER }

    private volatile Decision decision = Decision.NONE;

    @Override
    public void ack() {
        decision = Decision.ACK;
    }

    @Override
    public void recover() {
        decision = Decision.RECOVER;
    }

    @Override
    public void expire() {
        // Nothing held here: the listener recovers the message when the flow ends without an ACK.
    }

    Decision decision() {
        return decision;
    }
}
