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

import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.listener.api.event.AlterTableEvent;
import org.apache.gravitino.listener.api.event.CreateTableEvent;
import org.apache.gravitino.listener.api.event.DropTableEvent;
import org.apache.gravitino.listener.api.info.TableInfo;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.types.Types;

/**
 * End-to-end smoke test.
 *
 * <p>Stands up a real HTTP receiver, points a real {@link EventPublisherListener} at it, fires real
 * Gravitino event objects through {@code onPostEvent}, and asserts on what arrives over the wire.
 *
 * <p>No mocks and no stubs. If it passes, the plugin works.
 */
public final class SmokeTest {

  private static final List<String> received = new ArrayList<>();
  private static final List<String> authHeaders = new ArrayList<>();
  private static int failures = 0;

  public static void main(String[] args) throws Exception {
    CountDownLatch expect = new CountDownLatch(3);

    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/ingest",
        exchange -> {
          ByteArrayOutputStream buf = new ByteArrayOutputStream();
          exchange.getRequestBody().transferTo(buf);
          synchronized (received) {
            received.add(buf.toString(StandardCharsets.UTF_8));
            authHeaders.add(String.valueOf(exchange.getRequestHeaders().getFirst("Authorization")));
          }
          exchange.sendResponseHeaders(202, -1);
          exchange.close();
          expect.countDown();
        });
    server.start();
    int port = server.getAddress().getPort();
    System.out.println("receiver listening on 127.0.0.1:" + port);

    Map<String, String> conf = new LinkedHashMap<>();
    conf.put("url", "http://127.0.0.1:" + port + "/ingest");
    conf.put("allowInsecureTransport", "true"); // loopback only
    conf.put("source", "https://gravitino.test/smoke");
    conf.put("auth.type", "bearer");
    conf.put("auth.bearer.token", "smoke-token");
    conf.put("header.X-Tenant-Id", "acme-data-platform");

    EventPublisherListener listener = new EventPublisherListener();
    listener.init(conf);
    listener.start();

    NameIdentifier ident =
        NameIdentifier.of("enterprise", "analytics", "sales", "daily_orders");
    String user = "dana.okafor@example.com";

    listener.onPostEvent(new CreateTableEvent(user, ident, tableInfo()));
    listener.onPostEvent(
        new AlterTableEvent(
            user,
            ident,
            new TableChange[] {
              TableChange.addColumn(new String[] {"desk"}, Types.StringType.get(), "Owning desk")
            },
            tableInfo()));
    listener.onPostEvent(new DropTableEvent(user, ident, true));

    if (!expect.await(20, TimeUnit.SECONDS)) {
      System.out.println("TIMED OUT waiting for events. Got " + received.size() + " of 3.");
      failures++;
    }
    listener.stop();
    server.stop(0);

    System.out.println("\n=================== DELIVERED ===================\n");
    for (String body : received) {
      System.out.println(body);
      System.out.println();
    }
    System.out.println("=================================================\n");

    // Assertions on what actually crossed the wire.
    check(received.size() == 3, "three events delivered (got " + received.size() + ")");
    check(authHeaders.stream().allMatch("Bearer smoke-token"::equals), "every request authenticated");

    String create = received.get(0);
    check(create.contains("\"org.apache.gravitino.table.created\""), "create carries the right type");
    check(create.contains("\"customer_id\""), "create carries columns");
    check(create.contains("decimal(18,2)"), "create carries column types");
    check(create.contains("\"actor\":\"" + user + "\""), "create carries the actor");
    check(create.contains("\"subject\":\"enterprise.analytics.sales.daily_orders\""),
        "create carries the FQN as subject");
    check(create.contains("\"specversion\":\"1.0\""), "CloudEvents envelope present");
    check(create.contains("\"sequence\":1"), "sequence starts at 1");

    String alter = received.get(1);
    check(alter.contains("\"org.apache.gravitino.table.altered\""), "alter carries the right type");
    check(alter.contains("\"changes\""), "alter carries the change list");
    check(alter.contains("\"sequence\":2"), "sequence increments");

    String drop = received.get(2);
    check(drop.contains("\"org.apache.gravitino.table.dropped\""), "drop carries the right type");
    check(!drop.contains("\"data\""), "drop is a tombstone: no state, as DropTableEvent carries none");

    System.out.println(failures == 0 ? "\nALL CHECKS PASSED" : "\n" + failures + " CHECK(S) FAILED");
    System.exit(failures == 0 ? 0 : 1);
  }

  private static TableInfo tableInfo() {
    Column[] cols =
        new Column[] {
          Column.of("order_date", Types.DateType.get(), "Business date", false, false, null),
          Column.of("customer_id", Types.StringType.get(), "Customer key", false, false, null),
          Column.of("order_total", Types.DecimalType.of(18, 2), "Order total, USD", true, false, null),
        };
    Map<String, String> props = new LinkedHashMap<>();
    props.put("provider", "lakehouse-iceberg");
    props.put("location", "s3://example-lake/sales/daily_orders");
    props.put("classification", "internal");

    AuditInfo audit =
        AuditInfo.builder()
            .withCreator("analytics-eng")
            .withCreateTime(Instant.parse("2026-02-04T09:12:44Z"))
            .withLastModifier("dana.okafor@example.com")
            .withLastModifiedTime(Instant.parse("2026-07-13T18:42:11Z"))
            .build();

    return new TableInfo(
        "daily_orders",
        cols,
        "Daily order totals by customer",
        props,
        null,
        null,
        null,
        null,
        audit);
  }

  private static void check(boolean ok, String what) {
    System.out.println((ok ? "  PASS  " : "  FAIL  ") + what);
    if (!ok) {
      failures++;
    }
  }
}
