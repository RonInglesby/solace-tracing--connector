package com.solace.connectors.tracing.internal;

import java.util.HashMap;
import java.util.Map;

import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapSetter;

/** W3C trace-context helpers shared by the listener and the publish operation. */
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

    /** W3C strings ("traceparent", "tracestate") for the given context. */
    public static Map<String, String> inject(Context context) {
        Map<String, String> carrier = new HashMap<String, String>();
        PROPAGATOR.inject(context, carrier, MAP_SETTER);
        return carrier;
    }
}
