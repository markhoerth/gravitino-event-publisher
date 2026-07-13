# Gravitino Event Publisher

An example `EventListenerPlugin` for Apache Gravitino. Publishes catalog change events (table created, altered, dropped) to an external system over authenticated HTTP.

Built by Datastrato as a worked example. Not part of Apache Gravitino.

## What it does

When a table is created, altered, or dropped in Gravitino, the plugin POSTs a JSON document to an endpoint you configure. The document carries the full state of the table after the operation: columns, types, comments, properties, owner, and the user who made the change.

See `docs/sample-event.json` for exactly what arrives. It is captured output from a real run, not a hand-written example.

## Design

Three layers, and the separation is the point.

```
Gravitino Event  ──►  EventMapper  ──►  CatalogEvent  ──►  EventTransport  ──►  your endpoint
 (internal Java)      (translation)     (the payload)      (swappable)
```

`CatalogEvent` is the payload contract. `EventTransport` is how it leaves the process. A Kafka sink would produce the identical bytes to a topic; a JDBC sink would insert them. Adding one is a new class implementing `EventTransport`, not a new plugin, and the payload does not change.

Getting that boundary right matters. If each destination invents its own JSON, a downstream consumer has to special-case every one.

### Full state, not a diff

Every event carries the table as it exists after the operation, not a description of what changed. Your consumer can apply the payload blind, with no merge and no read-modify-write, which makes it idempotent: a replayed event is harmless, and a missed event means stale-until-next-write rather than permanently wrong.

The applied changes ride along in a secondary `changes` field for consumers that want them. No consumer should need that field to be correct.

Drops are the exception. Gravitino's `DropTableEvent` carries no table state, so a drop is a tombstone: name, timestamp, actor. Key the delete off `subject`.

### CloudEvents envelope

The payload is a CloudEvents v1.0 document. Off-the-shelf SDKs exist in every language, so you can ingest it without a bespoke client.

### Authentication is required

`auth.type` must be set. Supported: OAuth2 client credentials, mTLS, and static bearer. There is no unauthenticated mode.

## Delivery semantics

**At-most-once.** Stated plainly rather than left to be discovered.

Gravitino's async event dispatcher uses a bounded queue and drops on overflow. Nothing survives a server restart. Delivery failures are retried with jittered exponential backoff and then written to a dead-letter log.

If you need a gap-free record, reconcile periodically against the Gravitino REST API rather than trusting the event stream alone.

## Build

Three ways, depending on which Gravitino you are targeting. All of them run the same code.

**Gravitino 1.3** (the default):

```
./gradlew build
./gradlew smokeTest
```

**Gravitino 1.2:**

```
./gradlew build     -PgravitinoVersion=1.2.0
./gradlew smokeTest -PgravitinoVersion=1.2.0
```

**Against a local Gravitino install**, at whatever version it happens to be. Use this if Maven Central is not reachable, or if you would rather compile against the exact jars your server actually runs:

```
./gradlew build     -PgravitinoHome=/opt/gravitino
./gradlew smokeTest -PgravitinoHome=/opt/gravitino
```

The event listener API is identical across Gravitino 1.2 and 1.3. The plugin compiles and passes its smoke test on both, with no source change.

`smokeTest` stands up an HTTP receiver on a loopback port, points a real listener at it, fires real Gravitino `CreateTableEvent`, `AlterTableEvent`, and `DropTableEvent` objects through `onPostEvent`, and asserts on what arrives. No mocks. It is wired into `check`, so `build` runs it too.

### Toolchain

The wrapper pins Gradle 9.1.0 and downloads it on first run, so whatever Gradle is on your PATH does not matter.

Any JDK between 17 and 25 works. The build targets release 17 because that is what the Gravitino 1.x server runs, but it compiles from any newer JDK, so no separate JDK 17 install is needed. Gradle 9.1.0 is pinned specifically because it is the first release that can run on JDK 25; Gradle 8.x fails there with `Unsupported class file major version 69`.

## Try it in five minutes

`examples/receiver.py` is a fifty-line HTTP receiver with no dependencies. It checks the bearer
token and pretty-prints whatever arrives. Run it, point Gravitino at it, and watch a `CREATE TABLE`
land in your terminal.

```
# 1. build and install the plugin
./gradlew build -PgravitinoHome=$GRAVITINO_HOME
cp build/libs/gravitino-event-publisher-0.1.0.jar $GRAVITINO_HOME/libs/

# 2. append the local config
cat examples/gravitino.conf.local >> $GRAVITINO_HOME/conf/gravitino.conf

# 3. start the receiver (leave it running)
python3 examples/receiver.py

# 4. restart Gravitino, then create a table
$GRAVITINO_HOME/bin/gravitino.sh restart
```

The receiver and `examples/gravitino.conf.local` are a demo, not a product. The config uses a static
bearer token and plain HTTP over loopback. `conf/gravitino.conf.sample` is the real one.

If the plugin fails to load, Gravitino will not start and the log will say why. On success you will
see `event publisher initialised` and then `event publisher started` in
`$GRAVITINO_HOME/logs/gravitino-server.log`.

## Install

Drop the jar in Gravitino's `libs/` directory and add the configuration in `conf/gravitino.conf.sample` to your `gravitino.conf`. Restart the server.

The jar is thin. Gravitino, Jackson, and SLF4J are all `compileOnly`, because the server already provides them, and the HTTP client is `java.net.http` from the JDK. No shading, no relocation, nothing that can collide with the server's classpath.

## Configuration

See `conf/gravitino.conf.sample` for the full surface. The essentials:

| Property | Purpose |
|---|---|
| `url` | Where to POST |
| `source` | A stable URI identifying this Gravitino server. Appears as the CloudEvents `source` |
| `auth.type` | `oauth2`, `mtls`, or `bearer`. Required |
| `operations` | Which event types to publish. Defaults to mutations only |
| `maxAttempts` | Retry budget before dead-lettering |

Without the `operations` filter, `LOAD_TABLE` read traffic will swamp your consumer.

## Layout

```
src/main/java/com/datastrato/gravitino/eventpublisher/
├── EventPublisherListener.java      EventListenerPlugin impl: wiring, filtering, dead letters
├── wire/
│   ├── CatalogEvent.java            The payload. CloudEvents envelope, versioned.
│   ├── EntityState.java             Full table state, source-agnostic
│   └── EventMapper.java             Gravitino Event -> CatalogEvent. The only translation point.
└── transport/
    ├── EventTransport.java          Transport interface. The swap point.
    ├── HttpTransport.java           HTTP POST, retry, backoff
    └── AuthProvider.java            OAuth2 / mTLS / bearer

conf/gravitino.conf.sample           Full configuration surface
docs/sample-event.json               Captured output from a real run
examples/receiver.py                 Fifty-line receiver, no dependencies
examples/gravitino.conf.local        Config that pairs with it. Demo only.
```

## Status

Compiled and run against the Apache Gravitino 1.3.0 and 1.2.0 binary distributions. The smoke test passes on both.

Deployed and verified on a running Gravitino server. The plugin was installed into a live 1.3 deployment with OAuth2 authentication against Keycloak, registered through `gravitino.conf`, and confirmed to publish on a real `CREATE TABLE` against an Iceberg catalog. The delivered event carried the authenticated caller's identity in the `actor` field, taken from the request's bearer token rather than from any configuration:

```json
{
  "type": "org.apache.gravitino.table.created",
  "subject": "enterprise.analytics.sales.daily_orders",
  "actor": "service-account-user1",
  "actoraddress": "[0:0:0:0:0:0:0:1]",
  "eventsource": "gravitino_server"
}
```

Plugin registration, configuration loading, classloading, the operations filter, event dispatch, serialization, and authenticated HTTP delivery are all exercised on that path.

## License

Apache License 2.0. See `LICENSE`.

Apache Gravitino is a trademark of the Apache Software Foundation. This example is independent and is not endorsed by the ASF.
