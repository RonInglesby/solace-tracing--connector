# Solace Tracing Connector for Mule 4

The Solace Tracing connector is a Mule 4 connector that gives you a Solace **queue listener** and a **publish** operation. Both carry OpenTelemetry trace context in the Solace message's own (native) trace context. Mule flow spans and Solace broker spans then form one connected trace in Jaeger, or in any other OTLP backend.

It replaces three things:

- `jms:listener` and `jms:publish`;
- the Java Module calls;
- the `TraceHelper`, `SolacePublisher` and `SolaceConsumer` classes.

## Why it exists

When the broker relays a message, it replaces the trace context on that message with its own "send" span. A consumer only nests under the broker's spans if it reads that context. Reading it needs the raw `javax.jms.Message` and Solace's `SolaceJmsW3CTextMapGetter`. The Mule JMS connector doesn't expose the raw message, so this connector does the receive itself.

## What it does

**Listener** (`<solace-tracing:listener>`). It is push-based and works like `jms:listener`. For each message, it:

1. extracts the broker-updated trace context, falling back to the `traceparent` user property;
2. starts a CONSUMER span under that context;
3. runs the flow;
4. acks the message if the flow succeeds, or calls `recover()` so the broker redelivers it if the flow fails.

It also provides `attributes.traceparent` and `attributes.tracestate`, which identify the receive span. Pass these to `publish`.

**Publish** (`<solace-tracing:publish>`). It sends to a topic or a queue. It starts a PRODUCER span and writes the trace context in two places: the Solace-native context, and the `traceparent`/`tracestate` user properties as a fallback. It returns `traceparent`, `tracestate` and `messageId`.

Other behaviour:

- **Errors.** Publish raises `SOLACE-TRACING:PUBLISH` or `SOLACE-TRACING:CONNECTIVITY`. Connectivity errors trigger Mule reconnection.
- **Spans.** Spans are exported over OTLP/HTTP. There is one service name per listener or publish, set with `serviceName`, with a default on the config. The exporters are flushed and shut down when the app stops.

## Build and install

You need JDK 17 and Maven 3.9+. You also need access to Maven Central and the MuleSoft repository; the repositories are declared in the pom.

```bash
mvn clean install
```

This installs `com.solace.connectors:solace-tracing-connector:1.0.1` as a `mule-plugin` in your local Maven repository.

For CloudHub, Runtime Fabric, or team use, publish it to Anypoint Exchange:

1. Set the pom `groupId` to your Anypoint organization ID.
2. Run `mvn deploy` with the Exchange repository configured.

## Use it in the Mule app

1. **Add the dependency** to the app's `pom.xml`:

   ```xml
   <dependency>
       <groupId>com.solace.connectors</groupId>
       <artifactId>solace-tracing-connector</artifactId>
       <version>1.0.1</version>
       <classifier>mule-plugin</classifier>
   </dependency>
   ```

2. **Remove from the app what this connector replaces:**
   - the `sol-jms`, `javax.jms-api`, `solace-opentelemetry-jms-integration` and `io.opentelemetry` dependencies;
   - the Java Module, unless something else uses it;
   - `TraceHelper.java`, `SolacePublisher.java` and `SolaceConsumer.java`;
   - the `SOLACE_JMS_CONFIG` jms:config.

   Keep the JMS connector dependency only if other flows still use it.

3. **Use the example files in `mule-app/`.** These are `global.xml`, `orderExperience.xml`, `orderProcess.xml`, `orderSystem.xml` and `config.properties`.

## Broker setup for PROD

- **Redelivery and DMQ.** On every queue the listener consumes (`q1`, `q2`), set a **Maximum Redelivery Count** and attach a **Dead Message Queue**. A Solace queue's default is unlimited redelivery, so a message that always fails would otherwise retry forever. Publish sets `JMS_Solace_DeadMsgQueueEligible=true` by default (`dmqEligible`).
- **Distributed tracing.** It must be enabled on the Message VPN. The telemetry profile's trace filter needs subscriptions that match your topics, and its receiver must export to the same collector or Jaeger.
- **Concurrency.** `numberOfConsumers` above 1 needs a **non-exclusive** queue; an exclusive queue delivers to one consumer only. Each consumer uses its own connection.

## Delivery semantics

Delivery is at-least-once:

- **Ack timing.** A message is acked only after its flow completes successfully.
- **Duplicates.** A crash after publish but before ack causes redelivery, so the next hop can see a duplicate. Make consumers idempotent, for example by keying on `attributes.messageId` or a business key.
- **Stopping the app.** When the app stops, the listener waits up to `shutdownTimeoutMs` (default 30 s) for in-flight messages. Anything still running is recovered for redelivery.

## Not verified yet: check at first build

I wrote this against the documented Mule SDK and Solace APIs but couldn't compile it against the real jars. Check these points the first time you build:

1. **Solace connection factory setters.** The names used are `setConnectRetries`, `setReconnectRetries`, `setReconnectRetryWaitInMillis`, `setSSLValidateCertificate`, `setSSLTrustStore` and `setSSLTrustStorePassword`. If your `sol-jms` version names one differently, the compiler will point at it.
2. **`sol-jms` version.** The pom uses 10.17.0, the version Solace's compatibility table lists for integration library 1.2. Use the version your team standardises on, 10.17 or later.
3. **OpenTelemetry classloading.** The connector bundles OTel SDK 1.55. The Mule 4.12 runtime ships its own OTel 1.62 in `lib/opt`, but earlier testing showed app-bundled OTel was used. If you see `NoSuchMethodError` or `ClassCastException` in `io.opentelemetry`, that is the cause.
4. **Studio editor support.** After `mvn install`, refresh the app in Studio so the `solace-tracing` namespace and its palette entries appear.
"# solace-tracing--connector" 
