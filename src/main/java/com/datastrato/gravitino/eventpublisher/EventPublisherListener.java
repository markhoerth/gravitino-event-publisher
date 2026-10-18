/*
 * Copyright 2026 Datastrato Pvt Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Apache Gravitino is a trademark of the Apache Software Foundation.
 * This is an independent example and is not part of Apache Gravitino.
 */

package com.datastrato.gravitino.eventpublisher;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.gravitino.listener.api.EventListenerPlugin;
import org.apache.gravitino.listener.api.event.Event;
import org.apache.gravitino.listener.api.event.OperationStatus;
import com.datastrato.gravitino.eventpublisher.transport.EventTransport;
import com.datastrato.gravitino.eventpublisher.transport.HttpTransport;
import com.datastrato.gravitino.eventpublisher.transport.KafkaTransport;
import com.datastrato.gravitino.eventpublisher.wire.CatalogEvent;
import com.datastrato.gravitino.eventpublisher.wire.EventMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Publishes Gravitino catalog change events to an external system.
 *
 * <p>Reference implementation. The point of it is the shape, not the HTTP: a fixed wire contract
 * ({@link CatalogEvent}), a single mapper that owns the translation from internal event to that
 * contract, and a swappable {@link EventTransport} that carries the bytes. A Kafka sink is a new
 * transport and nothing else. It does not get to define a new payload.
 *
 * <h2>Mode</h2>
 *
 * <p>{@code ASYNC_ISOLATED}, deliberately, and this is the one design choice most likely to be
 * changed by someone who has not thought it through. The plugin performs network I/O with retries
 * and backoff. Under {@code SYNC} it would block the API call that produced the event, so a slow
 * consumer endpoint would slow down every {@code CREATE TABLE} in the cluster. Under {@code
 * ASYNC_SHARED} it would occupy the shared dispatcher thread for the whole retry window and starve
 * every other listener, including the audit log. Any plugin that talks to the network over a
 * retry loop belongs in its own dispatcher.
 *
 * <h2>Delivery semantics</h2>
 *
 * <p>At-most-once, and stated plainly rather than left to be discovered. Gravitino's async
 * dispatcher uses a bounded queue; on overflow, events are dropped. Delivery failures are retried
 * with jittered exponential backoff and then dead-lettered. Nothing here survives a server
 * restart. Any consumer that requires a complete, gap-free record must reconcile periodically
 * against the Gravitino API rather than trusting the event stream alone.
 */
public class EventPublisherListener implements EventListenerPlugin {

  private static final Logger LOG = LoggerFactory.getLogger(EventPublisherListener.class);

  /** Dead letters go here. A separate logger so operators can route it to its own file or sink. */
  private static final Logger DEAD_LETTER =
      LoggerFactory.getLogger("com.datastrato.gravitino.eventpublisher.DeadLetter");

  private final ObjectMapper json =
      new ObjectMapper()
          .registerModule(new JavaTimeModule())
          .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

  private EventMapper mapper;
  private EventTransport transport;

  /**
   * Event types this listener publishes, as lowercase {@code OperationType} names.
   *
   * <p>Filtering is not a nicety. Left unfiltered, a busy cluster produces {@code LOAD_TABLE} and
   * {@code LIST_TABLE} events by the thousand and drowns the consumer in reads it does not care
   * about. Default to mutations only.
   */
  private Set<String> enabledOperations;

  /** Whether to publish events for operations that failed. Off by default. */
  private boolean includeFailures;

  private final AtomicLong published = new AtomicLong();
  private final AtomicLong filtered = new AtomicLong();
  private final AtomicLong deadLettered = new AtomicLong();

  private static final String DEFAULT_OPERATIONS =
      "create_table,alter_table,drop_table,purge_table,"
          + "create_schema,alter_schema,drop_schema,"
          + "create_fileset,alter_fileset,drop_fileset,"
          + "register_model,delete_model,link_model_version";

  @Override
  public void init(Map<String, String> properties) {
    String source =
        properties.getOrDefault("source", "https://gravitino.apache.org/server/unknown");
    if ("https://gravitino.apache.org/server/unknown".equals(source)) {
      LOG.warn(
          "event publisher 'source' is not configured. Consumers fed by more than one Gravitino server "
              + "will be unable to tell them apart. Set it to a stable URI for this server.");
    }
    this.mapper = new EventMapper(source);

    this.enabledOperations =
        Collections.unmodifiableSet(
            new HashSet<>(
                Arrays.asList(
                    properties
                        .getOrDefault("operations", DEFAULT_OPERATIONS)
                        .toLowerCase(Locale.ROOT)
                        .split("\\s*,\\s*"))));

    this.includeFailures =
        Boolean.parseBoolean(properties.getOrDefault("includeFailures", "false"));

    // Transport selection. Every transport carries the same payload bytes, so nothing above this
    // line changes with the destination.
    String transportType = properties.getOrDefault("transport", "http");
    switch (transportType.toLowerCase(Locale.ROOT)) {
      case "http":
        this.transport = new HttpTransport();
        break;
      case "kafka":
        this.transport = new KafkaTransport();
        break;
      default:
        throw new IllegalArgumentException(
            "unsupported transport: " + transportType + ". Supported: http, kafka.");
    }
    this.transport.init(properties);

    LOG.info(
        "event publisher initialised: transport={}, operations={}, includeFailures={}",
        transport.name(),
        enabledOperations,
        includeFailures);
  }

  @Override
  public void start() {
    LOG.info("event publisher started");
  }

  @Override
  public void stop() {
    try {
      if (transport != null) {
        transport.close();
      }
    } catch (Exception e) {
      LOG.warn("error closing event publisher transport", e);
    }
    LOG.info(
        "event publisher stopped: published={}, filtered={}, deadLettered={}",
        published.get(),
        filtered.get(),
        deadLettered.get());
  }

  @Override
  public void onPostEvent(Event event) {
    if (!shouldPublish(event)) {
      filtered.incrementAndGet();
      return;
    }

    Optional<CatalogEvent> maybe = mapper.map(event);
    if (maybe.isEmpty()) {
      filtered.incrementAndGet();
      return;
    }
    CatalogEvent catalogEvent = maybe.get();

    byte[] payload;
    try {
      payload = json.writeValueAsString(catalogEvent).getBytes(StandardCharsets.UTF_8);
    } catch (Exception e) {
      // A serialization failure is a bug in the mapper or the schema, not a transient fault.
      // Retrying will not help. Dead-letter and move on.
      deadLetter(catalogEvent, null, e);
      return;
    }

    try {
      transport.send(catalogEvent, payload);
      published.incrementAndGet();
    } catch (EventTransport.TransportException e) {
      deadLetter(catalogEvent, payload, e);
    }
  }

  /**
   * Filters events before any work is done on them.
   *
   * <p>Two gates. Operation type, so read traffic never reaches the consumer. Operation status, so
   * a failed {@code CREATE TABLE} does not tell a downstream catalog that a table now exists when
   * it does not.
   */
  private boolean shouldPublish(Event event) {
    if (!includeFailures && event.operationStatus() != OperationStatus.SUCCESS) {
      return false;
    }
    String op = event.operationType().name().toLowerCase(Locale.ROOT);
    return enabledOperations.contains(op);
  }

  /**
   * Records an event that could not be delivered.
   *
   * <p>Explicit, and loud. The failure mode to avoid is a listener that swallows a delivery error,
   * returns normally, and leaves the operator believing the downstream catalog is current when it
   * has been diverging for a week. A dead letter is a signal that reconciliation is needed.
   *
   * <p>A log line is the floor, not the ceiling. A production deployment should route this to a
   * durable sink and alert on a non-zero rate.
   */
  private void deadLetter(CatalogEvent event, byte[] payload, Throwable cause) {
    deadLettered.incrementAndGet();
    DEAD_LETTER.error(
        "undeliverable event: id={}, type={}, subject={}, sequence={}, payload={}",
        event.id(),
        event.type(),
        event.subject(),
        event.sequence(),
        payload == null ? "<unserializable>" : new String(payload, StandardCharsets.UTF_8),
        cause);
  }

  /**
   * Isolated, because this plugin does network I/O with a retry loop.
   *
   * <p>See the class javadoc. Changing this to {@code ASYNC_SHARED} makes one unreachable consumer
   * endpoint capable of stalling every other listener on the server.
   */
  @Override
  public Mode mode() {
    return Mode.ASYNC_ISOLATED;
  }
}
