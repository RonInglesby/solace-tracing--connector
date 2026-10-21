package com.solace.connectors.tracing.internal;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.jms.Connection;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wraps one Solace JMS connection. Keeps a small pool of publishing sessions (JMS sessions are
 * single-threaded, so each publish borrows one) and fans connection failures out to listeners.
 */
public class SolaceConnection {

    private static final Logger LOGGER = LoggerFactory.getLogger(SolaceConnection.class);

    private final Connection connection;
    private final ConcurrentLinkedQueue<PublisherSession> idlePublishers = new ConcurrentLinkedQueue<PublisherSession>();
    private final List<ExceptionListener> listeners = new CopyOnWriteArrayList<ExceptionListener>();
    private volatile boolean valid = true;

    public SolaceConnection(Connection connection) throws JMSException {
        this.connection = connection;
        connection.setExceptionListener(new ExceptionListener() {
            @Override
            public void onException(JMSException e) {
                valid = false;
                LOGGER.warn("Solace connection failed: {}", e.getMessage());
                for (ExceptionListener listener : listeners) {
                    try {
                        listener.onException(e);
                    } catch (RuntimeException ex) {
                        LOGGER.warn("Connection exception listener failed", ex);
                    }
                }
            }
        });
    }

    public void start() throws JMSException {
        connection.start();
    }

    public Session createSession(int acknowledgeMode) throws JMSException {
        return connection.createSession(false, acknowledgeMode);
    }

    public void addExceptionListener(ExceptionListener listener) {
        listeners.add(listener);
    }

    public boolean isValid() {
        return valid;
    }

    public void invalidate() {
        valid = false;
    }

    PublisherSession borrowPublisher() throws JMSException {
        PublisherSession publisher = idlePublishers.poll();
        if (publisher != null) {
            return publisher;
        }
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        // Anonymous producer: the destination is given on each send.
        return new PublisherSession(session, session.createProducer(null));
    }

    void returnPublisher(PublisherSession publisher) {
        if (valid) {
            idlePublishers.offer(publisher);
        } else {
            publisher.close();
        }
    }

    void discardPublisher(PublisherSession publisher) {
        publisher.close();
    }

    public void close() {
        valid = false;
        PublisherSession publisher;
        while ((publisher = idlePublishers.poll()) != null) {
            publisher.close();
        }
        try {
            connection.close();
        } catch (JMSException e) {
            LOGGER.debug("Error closing Solace connection", e);
        }
    }

    static final class PublisherSession {
        final Session session;
        final MessageProducer producer;

        PublisherSession(Session session, MessageProducer producer) {
            this.session = session;
            this.producer = producer;
        }

        void close() {
            try {
                session.close();
            } catch (JMSException e) {
                LOGGER.debug("Error closing publisher session", e);
            }
        }
    }
}
