package com.solace.connectors.tracing.internal;

import java.util.HashMap;
import java.util.Map;

import javax.jms.JMSException;
import javax.jms.Message;

import com.solace.opentelemetry.javaagent.jms.SolaceJmsW3CTextMapGetter;
import com.solace.opentelemetry.javaagent.jms.SolaceJmsW3CTextMapSetter;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapSetter;

/**
 * Trace-context helpers shared by every component, so the hierarchy is built the same way everywhere:
 * outbound messages carry the Solace-native context (read and updated by the broker) plus traceparent/tracestate
 * user properties; inbound messages are parented on the Solace-native context, falling back to the properties.
 */
public final class Tracing {

    public static final W3CTraceContextPropagator PROPAGATOR = W3CTraceContextPropagator.getInstance();

    private static final TextMapGetter<Map<String, String>> MAP_GETTER = new TextMapGetter<Map<String, String>>() {
        @Override
        public Iterable<String> keys(Map<String, String> carrier) {
            return carrier.keySet();
        }

        @Override
        public String get(Map<String, String> carrier, String key) {
            return carrier == null ? null : carrier.get(key);
        }
    };

    private static final TextMapSetter<Map<String, String>> MAP_SETTER = new TextMapSetter<Map<String, String>>() {
        @Override
        public void set(Map<String, String> carrier, String key, String value) {
            if (carrier != null) {
                carrier.put(key, value);
            }
        }
    };

    /** Parent context read from a received message, and where it came from. */
    public static final class Extracted {
        public final Context context;
        public final String source;

        Extracted(Context context, String source) {
            this.context = context;
            this.source = source;
        }

        public boolean isValid() {
            return Span.fromContext(context).getSpanContext().isValid();
        }
    }

    private Tracing() {
    }

    /** Context from W3C strings; the root context when none are given (the next span starts a new trace). */
    public static Context extract(String traceparent, String tracestate) {
        Map<String, String> carrier = new HashMap<String, String>();
        if (traceparent != null && !traceparent.isEmpty()) {
            carrier.put("traceparent", traceparent);
        }
        if (tracestate != null && !tracestate.isEmpty()) {
            carrier.put("tracestate", tracestate);
        }
        // Start from root, not Context.current(), so a context left on a pooled thread can't leak in.
        return PROPAGATOR.extract(Context.root(), carrier, MAP_GETTER);
    }

    /** Parent context for a received message: the broker-updated Solace-native context, else our user properties. */
    public static Extracted extract(Message message) {
        Context solace = PROPAGATOR.extract(Context.root(), message, new SolaceJmsW3CTextMapGetter());
        if (Span.fromContext(solace).getSpanContext().isValid()) {
            return new Extracted(solace, "solace");
        }
        Context fallback = extract(stringProperty(message, "traceparent"), stringProperty(message, "tracestate"));
        return new Extracted(fallback,
                Span.fromContext(fallback).getSpanContext().isValid() ? "user-property" : "none");
    }

    /** W3C strings ("traceparent", "tracestate") for the given context. */
    public static Map<String, String> inject(Context context) {
        Map<String, String> carrier = new HashMap<String, String>();
        PROPAGATOR.inject(context, carrier, MAP_SETTER);
        return carrier;
    }

    /** Puts the context on an outbound message both ways; returns the W3C strings written. */
    public static Map<String, String> inject(Context context, Message message) throws JMSException {
        Map<String, String> out = inject(context);
        for (Map.Entry<String, String> entry : out.entrySet()) {
            message.setStringProperty(entry.getKey(), entry.getValue());
        }
        PROPAGATOR.inject(context, message, new SolaceJmsW3CTextMapSetter());
        return out;
    }

    /** Starts a messaging span with the standard attributes. */
    public static Span startSpan(Tracer tracer, String name, Context parent, SpanKind kind,
                                 String destination, String operation) {
        return tracer.spanBuilder(name)
                .setParent(parent)
                .setSpanKind(kind)
                .setAttribute(AttributeKey.stringKey("messaging.system"), "solace")
                .setAttribute(AttributeKey.stringKey("messaging.operation"), operation)
                .setAttribute(AttributeKey.stringKey("messaging.destination.name"), destination == null ? "" : destination)
                .startSpan();
    }

    private static String stringProperty(Message message, String name) {
        try {
            return message.getStringProperty(name);
        } catch (JMSException e) {
            return null;
        }
    }
}
