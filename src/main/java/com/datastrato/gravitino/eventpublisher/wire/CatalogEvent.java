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

package com.datastrato.gravitino.eventpublisher.wire;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/**
 * The Gravitino catalog change event, on the wire.
 *
 * <p>Structured as a CloudEvents v1.0 envelope. The envelope fields are fixed by the CloudEvents
 * spec and are consumable by any off-the-shelf CloudEvents SDK. Everything Gravitino-specific lives
 * under {@code data}.
 *
 * <p>Every transport emits this identical body. HTTP POSTs it, a Kafka sink would produce it to a
 * topic, a JDBC sink would serialize it into a column. The transport is a delivery detail. The
 * schema is the contract, and it is the thing downstream consumers code against.
 *
 * <p>The schema is versioned via {@code dataschema}. Additive changes keep the version. Any change
 * that breaks an existing consumer requires a version bump and a deprecation window.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class CatalogEvent {

  /** Current wire schema version. Bump on any breaking change. */
  public static final String SCHEMA_VERSION = "v1";

  public static final String SCHEMA_URI =
      "https://gravitino.apache.org/schemas/catalog-event/" + SCHEMA_VERSION + ".json";

  // ---------------------------------------------------------------------
  // CloudEvents v1.0 required context attributes
  // ---------------------------------------------------------------------

  /** CloudEvents spec version. Constant. */
  @JsonProperty("specversion")
  private final String specVersion = "1.0";

  /**
   * Unique identifier for this event. Stable across retries of the same event, so a consumer can
   * deduplicate on {@code (source, id)}.
   */
  @JsonProperty("id")
  private final String id;

  /**
   * The emitting Gravitino instance, as a URI. Identifies which server produced the event when a
   * consumer is fed by several. Configured, not derived.
   */
  @JsonProperty("source")
  private final String source;

  /**
   * The event type, in reverse-DNS form. Derived from the Gravitino {@code OperationType}.
   *
   * <p>Examples: {@code org.apache.gravitino.table.created}, {@code
   * org.apache.gravitino.table.altered}, {@code org.apache.gravitino.table.dropped}, {@code
   * org.apache.gravitino.schema.created}, {@code org.apache.gravitino.fileset.altered}.
   *
   * <p>Consumers subscribe on this. Producers must not repurpose an existing type to mean something
   * new.
   */
  @JsonProperty("type")
  private final String type;

  /** Event time, as an RFC 3339 timestamp. */
  @JsonProperty("time")
  private final Instant time;

  /** Points at the JSON Schema for the {@code data} payload. Carries the schema version. */
  @JsonProperty("dataschema")
  private final String dataSchema;

  /** Always {@code application/json}. */
  @JsonProperty("datacontenttype")
  private final String dataContentType = "application/json";

  /**
   * The fully qualified name of the entity that changed, as a CloudEvents subject. Consumers that
   * want to route or partition by entity use this without parsing {@code data}.
   *
   * <p>Format: {@code metalake.catalog.schema.table}.
   */
  @JsonProperty("subject")
  private final String subject;

  // ---------------------------------------------------------------------
  // Gravitino extension attributes.
  //
  // CloudEvents requires context attribute names, including extensions, to consist only of
  // lowercase ASCII letters and digits. The official CloudEvents Java SDK throws on anything
  // else, so camelCase here would break exactly the consumers the CloudEvents envelope exists
  // to serve. Keep these lowercase. Anything needing a richer name belongs inside `data`.
  // ---------------------------------------------------------------------

  /**
   * Monotonic sequence number, assigned by the emitting server.
   *
   * <p>Ordering guard. A consumer that has already applied sequence N for an entity discards
   * anything below N. Necessary because async delivery does not preserve order, and {@code
   * eventTime()} is millisecond-granular, so two changes to the same table inside one millisecond
   * are indistinguishable by time alone.
   *
   * <p>Caveat, and it is a real one: the sequence here is per-process, assigned at emit. It orders
   * events from one Gravitino server. It does not order events across a horizontally scaled
   * deployment, because each server keeps its own counter. A durable per-entity version supplied by
   * the catalog itself would be the complete answer; a plugin cannot provide one.
   */
  @JsonProperty("sequence")
  private final long sequence;

  /** The authenticated principal that performed the operation. */
  @JsonProperty("actor")
  private final String actor;

  /** Client IP, as captured by Gravitino at event construction. */
  @JsonProperty("actoraddress")
  private final String actorAddress;

  /** Which Gravitino surface emitted the event, for example the core server or the Iceberg REST server. */
  @JsonProperty("eventsource")
  private final String eventSource;

  /** The metalake the entity belongs to. */
  @JsonProperty("metalake")
  private final String metalake;

  /** The catalog the entity belongs to. */
  @JsonProperty("catalog")
  private final String catalog;

  // ---------------------------------------------------------------------
  // Payload
  // ---------------------------------------------------------------------

  /**
   * The entity, as it exists after the operation completed.
   *
   * <p>Full state, not a diff. Downstream catalogs are state-oriented: they want to know what the
   * table looks like now, not what changed about it. Emitting full state makes the consumer
   * idempotent, so a replayed event is harmless and a dropped event means stale-until-next-write
   * rather than permanently wrong.
   *
   * <p>Null on drop. Gravitino's {@code DropTableEvent} carries no {@code TableInfo}, so a drop is
   * necessarily a tombstone: identifier plus type plus time, no state. Consumers key the delete off
   * {@code subject}.
   */
  @JsonProperty("data")
  private final EntityState data;

  /**
   * The changes applied, when the source event exposes them. Present on alter operations, absent
   * otherwise.
   *
   * <p>Secondary, deliberately. Provided for consumers that want to react to a specific change (a
   * column drop, say) without diffing state themselves. A consumer must never need this field to
   * stay correct.
   */
  @JsonProperty("changes")
  private final java.util.List<Change> changes;

  /** Anything the emitting operation attached via {@code Event#customInfo()}. */
  @JsonProperty("custominfo")
  private final Map<String, String> customInfo;

  private CatalogEvent(Builder b) {
    this.id = Objects.requireNonNull(b.id, "id");
    this.source = Objects.requireNonNull(b.source, "source");
    this.type = Objects.requireNonNull(b.type, "type");
    this.time = Objects.requireNonNull(b.time, "time");
    this.subject = Objects.requireNonNull(b.subject, "subject");
    this.dataSchema = SCHEMA_URI;
    this.sequence = b.sequence;
    this.actor = b.actor;
    this.actorAddress = b.actorAddress;
    this.eventSource = b.eventSource;
    this.metalake = b.metalake;
    this.catalog = b.catalog;
    this.data = b.data;
    this.changes = b.changes;
    this.customInfo = b.customInfo;
  }

  public String id() {
    return id;
  }

  public String type() {
    return type;
  }

  public String subject() {
    return subject;
  }

  public long sequence() {
    return sequence;
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Builder for {@link CatalogEvent}. */
  public static final class Builder {
    private String id;
    private String source;
    private String type;
    private Instant time;
    private String subject;
    private long sequence;
    private String actor;
    private String actorAddress;
    private String eventSource;
    private String metalake;
    private String catalog;
    private EntityState data;
    private java.util.List<Change> changes;
    private Map<String, String> customInfo;

    public Builder id(String v) {
      this.id = v;
      return this;
    }

    public Builder source(String v) {
      this.source = v;
      return this;
    }

    public Builder type(String v) {
      this.type = v;
      return this;
    }

    public Builder time(Instant v) {
      this.time = v;
      return this;
    }

    public Builder subject(String v) {
      this.subject = v;
      return this;
    }

    public Builder sequence(long v) {
      this.sequence = v;
      return this;
    }

    public Builder actor(String v) {
      this.actor = v;
      return this;
    }

    public Builder actorAddress(String v) {
      this.actorAddress = v;
      return this;
    }

    public Builder eventSource(String v) {
      this.eventSource = v;
      return this;
    }

    public Builder metalake(String v) {
      this.metalake = v;
      return this;
    }

    public Builder catalog(String v) {
      this.catalog = v;
      return this;
    }

    public Builder data(EntityState v) {
      this.data = v;
      return this;
    }

    public Builder changes(java.util.List<Change> v) {
      this.changes = v;
      return this;
    }

    public Builder customInfo(Map<String, String> v) {
      this.customInfo = v;
      return this;
    }

    public CatalogEvent build() {
      return new CatalogEvent(this);
    }
  }

  /**
   * A single change applied by an alter operation.
   *
   * <p>Structured rather than a rendered string. Gravitino's {@code TableChange} does not override
   * {@code toString()}, so serializing it directly emits an object identity hash and tells the
   * consumer nothing.
   */
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public static final class Change {

    /** The kind of change, for example {@code AddColumn} or {@code UpdateColumnType}. */
    @JsonProperty("kind")
    private final String kind;

    /** The dotted path of the affected column, when the change targets one. */
    @JsonProperty("field")
    private final String field;

    public Change(String kind, String field) {
      this.kind = kind;
      this.field = field;
    }

    public String kind() {
      return kind;
    }

    public String field() {
      return field;
    }
  }
}
