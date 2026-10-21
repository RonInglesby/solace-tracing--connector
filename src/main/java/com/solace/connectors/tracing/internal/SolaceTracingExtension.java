package com.solace.connectors.tracing.internal;

import static org.mule.sdk.api.meta.JavaVersion.JAVA_17;

import org.mule.runtime.extension.api.annotation.Configurations;
import org.mule.runtime.extension.api.annotation.Extension;
import org.mule.runtime.extension.api.annotation.dsl.xml.Xml;
import org.mule.runtime.extension.api.annotation.error.ErrorTypes;
import org.mule.sdk.api.annotation.JavaVersionSupport;

/**
 * Solace Tracing connector: a queue listener and a publish operation that carry OpenTelemetry trace context
 * in the Solace-native message context, so Mule spans and Solace broker spans form one trace.
 *
 * XML namespace: http://www.mulesoft.org/schema/mule/solace-tracing (prefix "solace-tracing").
 */
@Xml(prefix = "solace-tracing")
@Extension(name = "Solace Tracing", vendor = "Solace")
@Configurations(SolaceTracingConfiguration.class)
@ErrorTypes(SolaceError.class)
@JavaVersionSupport({JAVA_17})
public class SolaceTracingExtension {
}
