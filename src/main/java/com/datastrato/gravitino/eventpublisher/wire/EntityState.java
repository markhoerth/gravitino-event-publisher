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
import java.util.List;
import java.util.Map;

/**
 * The full state of a Gravitino entity after an operation completed.
 *
 * <p>Source-agnostic on purpose. A table from a Hive catalog, an Iceberg table arriving over the
 * IRC, and a MySQL table all serialize into the same shape. A downstream business catalog should
 * not need to know or care which Gravitino catalog backend the table came from, and should not need
 * a different code path per source.
 *
 * <p>The breadth is the whole point of routing through Gravitino rather than pointing a business
 * catalog at each source system directly. Preserving it in the wire format is what makes that
 * breadth usable.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class EntityState {

  /** The kind of entity: {@code table}, {@code schema}, {@code fileset}, {@code model}, {@code topic}. */
  @JsonProperty("entityType")
  private String entityType;

  /** Fully qualified name: {@code metalake.catalog.schema.name}. Matches the envelope {@code subject}. */
  @JsonProperty("fqn")
  private String fqn;

  /** The entity's own name, unqualified. */
  @JsonProperty("name")
  private String name;

  /**
   * The catalog provider backing this entity: {@code hive}, {@code lakehouse-iceberg}, {@code
   * jdbc-mysql}, {@code kafka}, and so on. Informational for the consumer, never load-bearing.
   */
  @JsonProperty("provider")
  private String provider;

  /** Human-readable description. */
  @JsonProperty("comment")
  private String comment;

  /** Columns, in order. Null for entity types that have no columns. */
  @JsonProperty("columns")
  private List<ColumnState> columns;

  /** Partitioning expressions, rendered as strings. */
  @JsonProperty("partitioning")
  private List<String> partitioning;

  /** Sort order expressions, rendered as strings. */
  @JsonProperty("sortOrder")
  private List<String> sortOrder;

  /** All entity properties, verbatim. Includes storage location, format, and backend-specific keys. */
  @JsonProperty("properties")
  private Map<String, String> properties;

  /** Owner, from Gravitino's audit info. */
  @JsonProperty("owner")
  private String owner;

  /** Creation timestamp, RFC 3339. */
  @JsonProperty("createdAt")
  private String createdAt;

  /** Last modification timestamp, RFC 3339. */
  @JsonProperty("lastModifiedAt")
  private String lastModifiedAt;

  /** Last modifier, from Gravitino's audit info. */
  @JsonProperty("lastModifiedBy")
  private String lastModifiedBy;

  public EntityState entityType(String v) {
    this.entityType = v;
    return this;
  }

  public EntityState fqn(String v) {
    this.fqn = v;
    return this;
  }

  public EntityState name(String v) {
    this.name = v;
    return this;
  }

  public EntityState provider(String v) {
    this.provider = v;
    return this;
  }

  public EntityState comment(String v) {
    this.comment = v;
    return this;
  }

  public EntityState columns(List<ColumnState> v) {
    this.columns = v;
    return this;
  }

  public EntityState partitioning(List<String> v) {
    this.partitioning = v;
    return this;
  }

  public EntityState sortOrder(List<String> v) {
    this.sortOrder = v;
    return this;
  }

  public EntityState properties(Map<String, String> v) {
    this.properties = v;
    return this;
  }

  public EntityState owner(String v) {
    this.owner = v;
    return this;
  }

  public EntityState createdAt(String v) {
    this.createdAt = v;
    return this;
  }

  public EntityState lastModifiedAt(String v) {
    this.lastModifiedAt = v;
    return this;
  }

  public EntityState lastModifiedBy(String v) {
    this.lastModifiedBy = v;
    return this;
  }

  /** A single column, fully described. */
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public static final class ColumnState {
    @JsonProperty("name")
    private String name;

    /** The type, rendered via Gravitino's canonical simple-string form, for example {@code decimal(10,2)}. */
    @JsonProperty("type")
    private String type;

    @JsonProperty("comment")
    private String comment;

    @JsonProperty("nullable")
    private boolean nullable;

    @JsonProperty("autoIncrement")
    private boolean autoIncrement;

    /** Ordinal position, zero-based. Preserved because column order is semantically meaningful. */
    @JsonProperty("position")
    private int position;

    public ColumnState name(String v) {
      this.name = v;
      return this;
    }

    public ColumnState type(String v) {
      this.type = v;
      return this;
    }

    public ColumnState comment(String v) {
      this.comment = v;
      return this;
    }

    public ColumnState nullable(boolean v) {
      this.nullable = v;
      return this;
    }

    public ColumnState autoIncrement(boolean v) {
      this.autoIncrement = v;
      return this;
    }

    public ColumnState position(int v) {
      this.position = v;
      return this;
    }
  }
}
