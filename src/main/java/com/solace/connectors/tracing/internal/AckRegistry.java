package com.solace.connectors.tracing.internal;

import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pending manual acknowledgements, keyed by ackId. Handles not decided within the timeout are expired
 * (the message is redelivered), so an un-acked message can't hold a session open forever.
 */
public final class AckRegistry {

    private static final Logger LOGGER = LoggerFactory.getLogger(AckRegistry.class);

    private static final class Entry {
        final AckHandle handle;
        final long registeredAt;

        Entry(AckHandle handle, long registeredAt) {
            this.handle = handle;
            this.registeredAt = registeredAt;
        }
    }

    private final Map<String, Entry> entries = new ConcurrentHashMap<String, Entry>();
    private final long timeoutMs;

    public AckRegistry(long timeoutMs) {
        this.timeoutMs = Math.max(1000L, timeoutMs);
    }

    /** Registers a handle and returns its new ackId. */
    public String register(AckHandle handle) {
        sweep();
        String ackId = UUID.randomUUID().toString();
        entries.put(ackId, new Entry(handle, System.currentTimeMillis()));
        return ackId;
    }

    /** Removes and returns the handle, or null if unknown, already decided or expired. */
    public AckHandle remove(String ackId) {
        if (ackId == null) {
            return null;
        }
        Entry entry = entries.remove(ackId);
        return entry == null ? null : entry.handle;
    }

    private void sweep() {
        long cutoff = System.currentTimeMillis() - timeoutMs;
        Iterator<Map.Entry<String, Entry>> it = entries.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Entry> e = it.next();
            if (e.getValue().registeredAt < cutoff) {
                it.remove();
                LOGGER.warn("Manual ack {} not decided within {} ms; the message will be redelivered", e.getKey(), timeoutMs);
                e.getValue().handle.expire();
            }
        }
    }

    public void expireAll() {
        for (Entry entry : entries.values()) {
            entry.handle.expire();
        }
        entries.clear();
    }
}
