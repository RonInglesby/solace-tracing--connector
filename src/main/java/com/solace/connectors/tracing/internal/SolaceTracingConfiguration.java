package com.solace.connectors.tracing.internal;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.solace.connectors.tracing.internal.source.SolaceListener;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;

import org.mule.runtime.api.lifecycle.Disposable;
import org.mule.runtime.api.lifecycle.Initialisable;
import org.mule.runtime.api.lifecycle.InitialisationException;
import org.mule.runtime.extension.api.annotation.Operations;
import org.mule.runtime.extension.api.annotation.Sources;
import org.mule.runtime.extension.api.annotation.connectivity.ConnectionProviders;
import org.mule.runtime.extension.api.annotation.param.Optional;
import org.mule.runtime.extension.api.annotation.param.Parameter;
import org.mule.runtime.extension.api.annotation.param.display.DisplayName;
import org.mule.runtime.extension.api.annotation.param.display.Summary;

/**
 * Connector configuration. Owns the OpenTelemetry tracer providers (one per service.name) and the registry of
 * pending manual acknowledgements, and shuts both down cleanly when the app stops or is redeployed.
 */
@Operations(SolaceOperations.class)
@Sources(SolaceListener.class)
@ConnectionProviders(SolaceConnectionProvider.class)
public class SolaceTracingConfiguration implements Initialisable, Disposable {

    @Parameter
    @Optional(defaultValue = "mule-app")
    @DisplayName("Default service name")
    @Summary("service.name for spans when a component doesn't set its own")
    private String serviceName;

    @Parameter
    @Optional(defaultValue = "http://localhost:4318/v1/traces")
    @DisplayName("OTLP endpoint")
    @Summary("OTLP/HTTP traces endpoint of your OpenTelemetry Collector")
    private String otlpEndpoint;

    @Parameter
    @Optional(defaultValue = "true")
    @DisplayName("Tracing enabled")
    @Summary("When false, no spans are exported (trace context is still propagated if present)")
    private boolean tracingEnabled;

    @Parameter
    @Optional(defaultValue = "300000")
    @DisplayName("Manual ack timeout (ms)")
    @Summary("A message from Consume in MANUAL mode that isn't acked or recovered within this time is redelivered")
    private long manualAckTimeoutMs;

    private final Map<String, SdkTracerProvider> providers = new ConcurrentHashMap<String, SdkTracerProvider>();
    private final Map<String, Tracer> tracers = new ConcurrentHashMap<String, Tracer>();
    private volatile AckRegistry acks;

    @Override
    public void initialise() throws InitialisationException {
        acks = new AckRegistry(manualAckTimeoutMs);
    }

    @Override
    public void dispose() {
        if (acks != null) {
            acks.expireAll();
        }
        for (SdkTracerProvider provider : providers.values()) {
            provider.shutdown().join(5, TimeUnit.SECONDS);
        }
        providers.clear();
        tracers.clear();
    }

    /** Pending manual acknowledgements. */
    public AckRegistry acks() {
        AckRegistry registry = acks;
        if (registry == null) {
            synchronized (this) {
                if (acks == null) {
                    acks = new AckRegistry(manualAckTimeoutMs);
                }
                registry = acks;
            }
        }
        return registry;
    }

    /** Tracer for the given service name, or the configured default when null/blank. */
    public Tracer tracer(String serviceNameOverride) {
        if (!tracingEnabled) {
            return OpenTelemetry.noop().getTracer("solace-tracing-connector");
        }
        final String name = (serviceNameOverride == null || serviceNameOverride.trim().isEmpty())
                ? serviceName : serviceNameOverride.trim();
        Tracer tracer = tracers.get(name);
        if (tracer != null) {
            return tracer;
        }
        synchronized (this) {
            tracer = tracers.get(name);
            if (tracer == null) {
                OtlpHttpSpanExporter exporter = OtlpHttpSpanExporter.builder()
                        .setEndpoint(otlpEndpoint)
                        .setTimeout(10, TimeUnit.SECONDS)
                        .build();
                SdkTracerProvider provider = SdkTracerProvider.builder()
                        .setResource(Resource.getDefault().merge(Resource.create(
                                Attributes.of(AttributeKey.stringKey("service.name"), name))))
                        .addSpanProcessor(BatchSpanProcessor.builder(exporter).build())
                        .build();
                // Not registered globally, so it can't clash with the Mule runtime's own OpenTelemetry.
                OpenTelemetrySdk sdk = OpenTelemetrySdk.builder().setTracerProvider(provider).build();
                tracer = sdk.getTracer("solace-tracing-connector");
                providers.put(name, provider);
                tracers.put(name, tracer);
            }
            return tracer;
        }
    }
}
