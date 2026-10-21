# Solace Tracing Connector for Mule 4 (2.0.0)

The Solace Tracing connector is a Mule 4 connector that covers the same jobs as the MuleSoft JMS connector, for a Solace broker. Every component keeps the OpenTelemetry trace hierarchy intact across the broker:

- **Outbound messages** carry the trace context in two places: the Solace message's own (native) trace context, which the broker reads and updates, and `traceparent`/`tracestate` user properties as a fallback.
- **Inbound messages** get a CONSUMER span parented on the broker-updated context, so they nest under the broker's spans in Jaeger.

| Component | Replaces (JMS connector) | What it does |
|---|---|---|
| `listener` | On New Message | Push-based listener on a queue or topic, with AUTO, MANUAL or IMMEDIATE ack. It replies automatically when a message has `JMSReplyTo`. |
| `publish` | Publish | Sends to a topic or queue. Supports correlation ID, reply-to, priority, time to live, persistence, DMQ eligibility and user properties. |
| `consume` | Consume | Receives one message on demand, with a timeout and an optional selector. AUTO or MANUAL ack. |
| `publish-consume` | Publish consume | Request-reply over a temporary queue or a named reply queue. |
| `ack` | Ack | Acknowledges a message received in MANUAL mode. |
| `recover-session` | Recover session | Hands a MANUAL-mode message back to the broker for redelivery. |

Not included:

- **Durable topic subscribers.** On Solace, use a queue with a topic subscription instead (`listener`, `consumerType="QUEUE"`). This is the recommended Solace pattern; it survives restarts and allows several consumers.
- **JMS transactions and XA.** These aren't included either.

## Build and install

You need JDK 17 and Maven 3.9+, plus access to Maven Central and the MuleSoft repository.

```bash
mvn clean install
```

App dependency:

```xml
<dependency>
    <groupId>com.solace.connectors</groupId>
    <artifactId>solace-tracing-connector</artifactId>
    <version>2.0.0</version>
    <classifier>mule-plugin</classifier>
</dependency>
```

For CloudHub, Runtime Fabric, or team use, publish it to Anypoint Exchange:

1. Set the pom `groupId` to your Anypoint organization ID.
2. Run `mvn deploy`.

### Upgrading from 1.0.1

The listener's `queueName` attribute is now `destination`. Everything else is compatible. The `mule-app/` folder has the order flows already updated.

## How the trace hierarchy is kept

| Step | Span | Parent |
|---|---|---|
| `publish` | PRODUCER `<dest> send` | `traceparent` you pass (or a new trace) |
| broker | broker spans | the publish span (Solace-native context) |
| `listener` / `consume` | CONSUMER `<dest> receive` | broker's span (falls back to the `traceparent` property) |
| listener reply | PRODUCER `<replyTo> send` | the receive span |
| `publish-consume` | CLIENT `<dest> request` | `traceparent` you pass |
| `publish-consume` reply | CONSUMER `<dest> reply` | the reply's context (responder, then broker), else the request span |

The rule for flows: **pass `#[attributes.traceparent]` and `#[attributes.tracestate]` from whatever received the message into the next `publish` or `publish-consume`**. The `traceContextSource` attribute tells you where the parent came from:

- `solace` — the broker-updated context, which is what you want;
- `user-property` — the broker didn't trace the message;
- `none` — a new trace started.

## Component reference

### Configuration

```xml
<solace-tracing:config name="Solace_Tracing_Config" serviceName="mule-orders" otlpEndpoint="${otel.endpoint}">
    <solace-tracing:connection host="${solace.host}" vpn="${solace.vpn}"
        username="${solace.username}" password="${secure::solace.password}"
        reconnectRetries="20" reconnectRetryWaitMs="3000">
        <reconnection><reconnect-forever frequency="5000"/></reconnection>
    </solace-tracing:connection>
</solace-tracing:config>
```

Configuration settings:

- `serviceName` (default `mule-app`) — the default service name for spans.
- `otlpEndpoint` — where spans are sent.
- `tracingEnabled` (default `true`).
- `manualAckTimeoutMs` (default 300000) — a `consume` MANUAL message that isn't acked or recovered within this time is redelivered.

Connection settings:

- `host`, `vpn`, `username` and `password`.
- `connectRetries` (3), `reconnectRetries` (20; -1 means forever) and `reconnectRetryWaitMs` (3000) — retries made by the Solace API itself before Mule's reconnection takes over.
- `validateCertificate` (true), `trustStore` and `trustStorePassword` — TLS settings for `tcps://`.

### listener

```xml
<solace-tracing:listener config-ref="Solace_Tracing_Config" destination="q1"
    serviceName="mule-process" spanName="q1 receive"/>
```

| Parameter | Default | Notes |
|---|---|---|
| `destination` | — | Queue name, or topic (wildcards allowed) for TOPIC |
| `consumerType` | `QUEUE` | `TOPIC` = non-durable topic subscription |
| `ackMode` | `AUTO` | See below |
| `selector` | — | JMS message selector |
| `numberOfConsumers` | 1 | Queues only; needs a non-exclusive queue for more than 1 |
| `contentType` | `application/json` | Media type of the payload |
| `sendReplies` | `true` | Reply with the flow result when the message has `JMSReplyTo` |
| `spanName`, `serviceName` | — | Span naming |
| `shutdownTimeoutMs` | 30000 | How long stop waits for in-flight messages |

Ack modes:

- **AUTO** — the message is acked when the flow succeeds, and redelivered when it fails. Delivery is at-least-once.
- **MANUAL** — the flow calls `ack` (or `recover-session`) with `attributes.ackId`. The decision is applied when the flow finishes, and a flow that ends without an ack has its message redelivered.
- **IMMEDIATE** — the message is acked on receipt, before the flow runs. Delivery is at-most-once.

**Reply.** When the message has a `JMSReplyTo`, the flow's final payload is sent back with `JMSCorrelationID` set to the request's correlation ID, or its message ID if it has none. To send something other than the payload:

```xml
<solace-tracing:listener config-ref="Solace_Tracing_Config" destination="quote/requests">
    <solace-tracing:response>
        <solace-tracing:reply-body>#[vars.quote]</solace-tracing:reply-body>
    </solace-tracing:response>
</solace-tracing:listener>
```

Attributes on the message: `messageId`, `correlationId`, `destination`, `replyTo`, `redelivered`, `timestamp`, `priority`, `ackId` (MANUAL only), `traceparent`, `tracestate`, `traceContextSource` and `properties` (a map of all JMS properties).

### publish

```xml
<solace-tracing:publish config-ref="Solace_Tracing_Config" destination="order/processed"
    serviceName="mule-process" spanName="order/processed send"
    traceparent="#[attributes.traceparent]" tracestate="#[attributes.tracestate]"
    target="outTrace"/>
```

Parameters:

- `destination` and `destinationType` (`TOPIC` by default, or `QUEUE`).
- Body — the payload by default.
- `traceparent` and `tracestate` — the parent span for this publish.
- `correlationId`, `replyTo` and `replyToType`.
- `persistent` (true), `priority` (4), `timeToLive` (0 means it never expires), `dmqEligible` (true) and `userProperties` (a map).

It returns `traceparent`, `tracestate` and `messageId`. Errors: `SOLACE-TRACING:PUBLISH` and `SOLACE-TRACING:CONNECTIVITY`.

### consume

```xml
<flow name="nextOrder">
    <http:listener config-ref="HTTP_Listener_config_exp_api" path="orders/next"/>
    <solace-tracing:consume config-ref="Solace_Tracing_Config" destination="q4" maximumWait="2000"
        serviceName="mule-orders"/>
    <!-- payload = message body, attributes = SolaceMessageAttributes -->
</flow>
```

Parameters:

- `destination`, `consumerType` (QUEUE) and `selector`.
- `maximumWait` — milliseconds; -1 waits forever and 0 doesn't wait.
- `ackMode` — `AUTO` acks on receipt; `MANUAL` waits for `ack` or `recover-session`.
- `contentType`, `spanName` and `serviceName`.

Errors: `SOLACE-TRACING:TIMEOUT` (nothing arrived in time), `CONSUMING` and `CONNECTIVITY`.

### publish-consume (request-reply)

```xml
<solace-tracing:publish-consume config-ref="Solace_Tracing_Config" destination="quote/requests"
    destinationType="QUEUE" maximumWait="5000" serviceName="mule-orders"
    traceparent="#[attributes.traceparent]" tracestate="#[attributes.tracestate]"/>
```

By default it creates a temporary reply queue for each call. Set `replyTo` to use a named queue instead; replies are then matched on `JMSCorrelationID`.

The payload becomes the reply body, and `attributes` are those of the reply message. The responder can be a `listener` with `sendReplies="true"`, the default.

Errors: `TIMEOUT`, `PUBLISH`, `CONSUMING` and `CONNECTIVITY`.

Temporary queues need the client profile to allow guaranteed endpoint creation (**Allow Guaranteed Endpoint Create**).

### ack and recover-session (MANUAL mode)

```xml
<flow name="manualAck">
    <solace-tracing:listener config-ref="Solace_Tracing_Config" destination="q5" ackMode="MANUAL"/>
    <flow-ref name="processOrder"/>
    <solace-tracing:ack config-ref="Solace_Tracing_Config" ackId="#[attributes.ackId]"/>
    <error-handler>
        <on-error-continue>
            <solace-tracing:recover-session config-ref="Solace_Tracing_Config" ackId="#[attributes.ackId]"/>
        </on-error-continue>
    </error-handler>
</flow>
```

These raise `SOLACE-TRACING:ACK` if the `ackId` is unknown — that is, already acked or recovered, timed out, or not from MANUAL mode.

## Broker setup for PROD

- **Redelivery and DMQ.** Set a **Maximum Redelivery Count** and a **Dead Message Queue** on every queue the listener consumes. Otherwise a message that always fails is retried forever. Publish marks messages DMQ-eligible by default.
- **Distributed tracing.** It must be enabled on the Message VPN. The telemetry profile's trace filter needs subscriptions that match your topics and queues' topics, and its receiver must export to the same collector or Jaeger.
- **Concurrency.** `numberOfConsumers` above 1 needs a **non-exclusive** queue.
- **Request-reply.** `publish-consume` with temporary queues needs **Allow Guaranteed Endpoint Create** on the client profile.

## Delivery semantics

- **AUTO and MANUAL are at-least-once.** A crash after publish but before ack causes redelivery, so make consumers idempotent, for example by keying on `messageId` or a business key.
- **IMMEDIATE is at-most-once.**
- **Stopping the app.** The listener waits up to `shutdownTimeoutMs` for in-flight messages. Anything still running is redelivered.

## Check at first build

This was written against the documented Mule SDK and Solace APIs, but it hasn't been compiled against the real jars:

1. **Solace connection factory setters.** The names used are `setConnectRetries`, `setReconnectRetries`, `setReconnectRetryWaitInMillis`, `setSSLValidateCertificate`, `setSSLTrustStore` and `setSSLTrustStorePassword`. If your `sol-jms` version names one differently, the compiler will point at it.
2. **`sol-jms` version.** The pom uses 10.17.0, the version Solace's compatibility table lists for integration library 1.2. Use your team's standard, 10.17 or later.
3. **The listener's reply body element.** This is the `@OnSuccess` content parameter. If Studio shows a different element name than `reply-body` inside `<solace-tracing:response>`, use the name Studio shows. The default (payload) needs no XML at all.
4. **OpenTelemetry classloading.** The connector bundles OTel SDK 1.55, while the Mule runtime ships its own copy. A `NoSuchMethodError` from `io.opentelemetry` points to a class conflict.
