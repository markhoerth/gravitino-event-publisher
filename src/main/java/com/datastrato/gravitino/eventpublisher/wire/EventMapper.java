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

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.gravitino.Audit;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.listener.api.event.AlterTableEvent;
import org.apache.gravitino.listener.api.event.CreateTableEvent;
import org.apache.gravitino.listener.api.event.DropTableEvent;
import org.apache.gravitino.listener.api.event.Event;
import org.apache.gravitino.listener.api.info.TableInfo;

/**
 * Translates Gravitino's internal event objects into the {@link CatalogEvent} wire contract.
 *
 * <p>The single point at which internal representation becomes external contract. Every sink calls
 * this and emits what it produces. No sink is permitted to build its own JSON, which is exactly the
 * failure mode the reference exists to prevent: three sinks, three payloads, and a downstream
 * consumer that has to special-case each one.
 *
 * <p>The mapper is deliberately generic. It keys on {@code OperationType} rather than on the
 * concrete event class, so a new entity type added to core produces a well-formed event with no
 * change here. State extraction is best-effort by known type, and an unrecognized event still emits
 * a valid envelope with identifier, actor, and time.
 */
public final class EventMapper {

  private final String source;

  /**
   * Emit-order sequence.
   *
   * <p>Per-process, and therefore only a partial order. Correct for a single Gravitino server,
   * insufficient for a scaled-out deployment behind a load balancer. A durable per-entity version
   * supplied by the catalog itself would be the complete answer; a plugin cannot provide one.
   */
  private final AtomicLong sequence = new AtomicLong();

  public EventMapper(String source) {
    this.source = source;
  }

  /**
   * Maps a Gravitino post-event to the wire contract.
   *
   * @param event the internal event
   * @return the wire event, or empty if the event carries no identifier and cannot be addressed
   */
  public Optional<CatalogEvent> map(Event event) {
    NameIdentifier ident = event.identifier();
    if (ident == null) {
      return Optional.empty();
    }

    String[] parts = fqnParts(ident);
    String entityType = entityTypeOf(event);

    CatalogEvent.Builder builder =
        CatalogEvent.builder()
            .id(UUID.randomUUID().toString())
            .source(source)
            .type(cloudEventType(event, entityType))
            .time(Instant.ofEpochMilli(event.eventTime()))
            .subject(String.join(".", parts))
            .sequence(sequence.incrementAndGet())
            .actor(event.user())
            .actorAddress(event.remoteAddress())
            .eventSource(event.eventSource().name().toLowerCase(Locale.ROOT))
            .metalake(parts.length > 0 ? parts[0] : null)
            .catalog(parts.length > 1 ? parts[1] : null)
            .customInfo(emptyToNull(event.customInfo()));

    // State extraction. Full state where the event carries it, tombstone where it does not.
    if (event instanceof CreateTableEvent) {
      builder.data(tableState(((CreateTableEvent) event).createdTableInfo(), ident, parts));

    } else if (event instanceof AlterTableEvent) {
      AlterTableEvent alter = (AlterTableEvent) event;
      builder.data(tableState(alter.updatedTableInfo(), ident, parts));
      builder.changes(
          Arrays.stream(alter.tableChanges()).map(this::renderChange).collect(Collectors.toList()));

    } else if (event instanceof DropTableEvent) {
      // Deliberately no data. DropTableEvent exposes only isExists(), so a drop is a tombstone:
      // identifier, type, time. The consumer keys the delete off subject.
      Map<String, String> info = new LinkedHashMap<>();
      if (event.customInfo() != null) {
        info.putAll(event.customInfo());
      }
      info.put("existed", String.valueOf(((DropTableEvent) event).isExists()));
      builder.customInfo(info);
    }

    return Optional.of(builder.build());
  }

  /**
   * Renders one {@link TableChange} as a structured object.
   *
   * <p>Gravitino's {@code TableChange} implementations do not override {@code toString()}, so
   * calling it emits an object identity hash. Derive the kind from the class name and the affected
   * column from {@link TableChange.ColumnChange#fieldName()}, which every column-targeting change
   * implements. Changes that do not target a column, such as {@code RenameTable} and {@code
   * SetProperty}, carry a kind and no field.
   */
  private CatalogEvent.Change renderChange(TableChange change) {
    String kind = change.getClass().getSimpleName();
    String field = null;
    if (change instanceof TableChange.ColumnChange) {
      String[] path = ((TableChange.ColumnChange) change).fieldName();
      if (path != null && path.length > 0) {
        field = String.join(".", path);
      }
    }
    return new CatalogEvent.Change(kind, field);
  }

  /**
   * Renders the CloudEvents type, in reverse-DNS form.
   *
   * <p>Derived mechanically from Gravitino's {@code OperationType} so that new operations get a
   * sensible type with no change here. {@code CREATE_TABLE} becomes {@code
   * org.apache.gravitino.table.created}.
   */
  private String cloudEventType(Event event, String entityType) {
    String op = event.operationType().name().toLowerCase(Locale.ROOT);
    String verb;
    if (op.startsWith("create")) {
      verb = "created";
    } else if (op.startsWith("alter")) {
      verb = "altered";
    } else if (op.startsWith("drop")) {
      verb = "dropped";
    } else if (op.startsWith("purge")) {
      verb = "purged";
    } else {
      verb = op;
    }
    return "org.apache.gravitino." + entityType + "." + verb;
  }

  /** Derives the entity type from the operation, for example {@code CREATE_TABLE} to {@code table}. */
  private String entityTypeOf(Event event) {
    String op = event.operationType().name().toLowerCase(Locale.ROOT);
    int underscore = op.indexOf('_');
    return underscore >= 0 ? op.substring(underscore + 1) : op;
  }

  private EntityState tableState(TableInfo info, NameIdentifier ident, String[] parts) {
    EntityState state =
        new EntityState()
            .entityType("table")
            .fqn(String.join(".", parts))
            .name(info.name())
            .provider(providerOf(info))
            .comment(info.comment())
            .properties(info.properties())
            .columns(columnStates(info.columns()));

    if (info.partitioning() != null && info.partitioning().length > 0) {
      state.partitioning(
          Arrays.stream(info.partitioning()).map(Object::toString).collect(Collectors.toList()));
    }
    if (info.sortOrder() != null && info.sortOrder().length > 0) {
      state.sortOrder(
          Arrays.stream(info.sortOrder()).map(Object::toString).collect(Collectors.toList()));
    }

    Audit audit = info.auditInfo();
    if (audit != null) {
      state.owner(audit.creator());
      state.createdAt(audit.createTime() != null ? audit.createTime().toString() : null);
      state.lastModifiedBy(audit.lastModifier());
      state.lastModifiedAt(
          audit.lastModifiedTime() != null ? audit.lastModifiedTime().toString() : null);
    }
    return state;
  }

  /**
   * The catalog backend behind this entity, when the source system discloses it.
   *
   * <p>Informational for the consumer, never load-bearing. Absent when the backend does not
   * populate a provider hint, which is the common case for JDBC-backed catalogs.
   */
  private String providerOf(TableInfo info) {
    Map<String, String> props = info.properties();
    if (props == null) {
      return null;
    }
    for (String key : new String[] {"provider", "table-type", "format"}) {
      String v = props.get(key);
      if (v != null && !v.isEmpty()) {
        return v;
      }
    }
    return null;
  }

  private List<EntityState.ColumnState> columnStates(Column[] columns) {
    if (columns == null) {
      return null;
    }
    List<EntityState.ColumnState> out = new ArrayList<>(columns.length);
    for (int i = 0; i < columns.length; i++) {
      Column c = columns[i];
      out.add(
          new EntityState.ColumnState()
              .name(c.name())
              .type(c.dataType().simpleString())
              .comment(c.comment())
              .nullable(c.nullable())
              .autoIncrement(c.autoIncrement())
              .position(i));
    }
    return out;
  }

  /** Flattens a NameIdentifier into its FQN segments: metalake, catalog, schema, name. */
  private String[] fqnParts(NameIdentifier ident) {
    String[] levels = ident.namespace().levels();
    String[] parts = new String[levels.length + 1];
    System.arraycopy(levels, 0, parts, 0, levels.length);
    parts[levels.length] = ident.name();
    return parts;
  }

  private Map<String, String> emptyToNull(Map<String, String> m) {
    return (m == null || m.isEmpty()) ? null : m;
  }
}
