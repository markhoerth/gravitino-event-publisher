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

package com.datastrato.gravitino.eventpublisher.transport;

import java.io.Closeable;
import java.util.Map;
import com.datastrato.gravitino.eventpublisher.wire.CatalogEvent;

/**
 * How a {@link CatalogEvent} leaves the process.
 *
 * <p>The separation that the whole reference exists to demonstrate. The wire schema is the product
 * and it is fixed. The transport is a delivery mechanism and it is swappable. An HTTP transport
 * POSTs the serialized event to an endpoint. A Kafka transport would produce the identical bytes to
 * a topic, keyed on {@link CatalogEvent#subject()}. A JDBC transport would insert them. None of
 * them get to invent their own payload.
 *
 * <p>Get this boundary wrong and every new transport ships a new JSON shape. A consumer fed by two
 * of them would then have to special-case each one, which defeats the purpose of having a schema at
 * all. Every destination codes against a single contract.
 *
 * <p>Implementations must be thread-safe. They are called from the listener dispatcher thread.
 */
public interface EventTransport extends Closeable {

  /**
   * Configures the transport. Called once, during plugin init, before any {@link #send}.
   *
   * <p>Must fail fast and loudly on invalid or missing configuration. A misconfigured transport that
   * silently no-ops is worse than a server that refuses to start, because the operator believes
   * events are flowing when they are not.
   *
   * @param properties transport configuration, already stripped of the listener prefix
   * @throws IllegalArgumentException if configuration is missing or invalid
   */
  void init(Map<String, String> properties);

  /**
   * Delivers one event.
   *
   * <p>Synchronous with respect to the caller. Retry and backoff live in the implementation, and the
   * implementation is responsible for bounding total time spent so that one unreachable endpoint
   * cannot stall the dispatcher indefinitely.
   *
   * @param event the event to deliver
   * @param payload the serialized event body, produced once by the listener and shared across
   *     transports so that every sink emits byte-identical content
   * @throws TransportException if delivery failed after exhausting retries. The listener routes the
   *     event to the dead-letter sink. Never swallow a failure and return normally.
   */
  void send(CatalogEvent event, byte[] payload) throws TransportException;

  /** A human-readable name for this transport, used in logs and metrics. */
  String name();

  /** Raised when delivery fails permanently, after retries are exhausted. */
  class TransportException extends Exception {
    private static final long serialVersionUID = 1L;

    public TransportException(String message, Throwable cause) {
      super(message, cause);
    }

    public TransportException(String message) {
      super(message);
    }
  }
}
