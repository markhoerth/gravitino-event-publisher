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

import com.datastrato.gravitino.eventpublisher.transport.EventTransport;
import com.datastrato.gravitino.eventpublisher.transport.KafkaTransport;
import com.datastrato.gravitino.eventpublisher.wire.CatalogEvent;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.listener.api.event.AlterTableEvent;
import org.apache.gravitino.listener.api.event.CreateTableEvent;
import org.apache.gravitino.listener.api.event.DropTableEvent;
import org.apache.gravitino.listener.api.info.TableInfo;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.types.Types;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * End-to-end test of the Kafka transport against a real broker.
 *
 * <p>Expects a single-node broker on 127.0.0.1:19093 that accepts SASL_SSL with the PLAIN mechanism
 * and a user {@code gravitino} with password {@code gravitino-secret}, its certificate trusted by
 * {@code client-trust.p12} in the directory named by the {@code broker.dir} system property. Fires
 * real Gravitino event objects through a real {@link EventPublisherListener}, reads the topic back
 * with a consumer, and asserts on what the broker stored. No mocks.
 */
public final class KafkaSmokeTest {

  private static final String BOOTSTRAP = "127.0.0.1:19093";
  private static int failures = 0;

  public static void main(String[] args) throws Exception {
    String brokerDir = System.getProperty("broker.dir");
    String topic = "gravitino-catalog-events-" + UUID.randomUUID().toString().substring(0, 8);

    Map<String, String> security = new LinkedHashMap<>();
    security.put("security.protocol", "SASL_SSL");
    security.put("sasl.mechanism", "PLAIN");
    security.put(
        "sasl.jaas.config",
        "org.apache.kafka.common.security.plain.PlainLoginModule required "
            + "username=\"gravitino\" password=\"gravitino-secret\";");
    security.put("ssl.truststore.location", brokerDir + "/client-trust.p12");
    security.put("ssl.truststore.type", "PKCS12");
    security.put("ssl.truststore.password", "trustpass");
    security.put("ssl.endpoint.identification.algorithm", "");

    createTopic(topic, security);

    // 1. Publish real events through the listener over SASL_SSL.
    Map<String, String> conf = new LinkedHashMap<>();
    conf.put("source", "https://gravitino.test/kafka-smoke");
    conf.put("transport", "kafka");
    conf.put("bootstrapServers", BOOTSTRAP);
    conf.put("topic", topic);
    security.forEach((k, v) -> conf.put("kafka." + k, v));

    EventPublisherListener listener = new EventPublisherListener();
    listener.init(conf);
    listener.start();

    NameIdentifier ident = NameIdentifier.of("enterprise", "analytics", "sales", "daily_orders");
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
    listener.stop();

    // 2. Read back what the broker stored.
    List<ConsumerRecord<String, byte[]>> records = consume(topic, security, 3);

    System.out.println("\n=================== STORED IN " + topic + " ===================\n");
    for (ConsumerRecord<String, byte[]> r : records) {
      System.out.println(
          "partition=" + r.partition() + " offset=" + r.offset() + " key=" + r.key());
      System.out.println(new String(r.value(), StandardCharsets.UTF_8));
      System.out.println();
    }
    System.out.println("==========================================================\n");

    check(records.size() == 3, "three records stored (got " + records.size() + ")");
    if (records.size() == 3) {
      String subject = "enterprise.analytics.sales.daily_orders";
      check(
          records.stream().allMatch(r -> subject.equals(r.key())),
          "every record keyed by the entity's subject");
      check(
          records.stream().map(ConsumerRecord::partition).distinct().count() == 1,
          "all events for one table on one partition");
      check(
          records.stream()
              .allMatch(r -> "application/cloudevents+json; charset=UTF-8".equals(header(r, "content-type"))),
          "content-type header marks a structured CloudEvent");

      String create = new String(records.get(0).value(), StandardCharsets.UTF_8);
      String alter = new String(records.get(1).value(), StandardCharsets.UTF_8);
      String drop = new String(records.get(2).value(), StandardCharsets.UTF_8);
      check(create.contains("\"specversion\":\"1.0\""), "CloudEvents envelope present");
      check(create.contains("\"org.apache.gravitino.table.created\""), "first record is the create");
      check(create.contains("\"customer_id\"") && create.contains("decimal(18,2)"),
          "create carries columns and types");
      check(create.contains("\"actor\":\"" + user + "\""), "create carries the actor");
      check(alter.contains("\"org.apache.gravitino.table.altered\""), "second record is the alter");
      check(drop.contains("\"org.apache.gravitino.table.dropped\""), "third record is the drop");
      check(!drop.contains("\"data\""), "drop is a tombstone");
      check(create.contains("\"sequence\":1") && alter.contains("\"sequence\":2")
          && drop.contains("\"sequence\":3"), "records stored in commit order");
    }

    // 3. Configurations without authentication are refused at startup.
    check(
        refused(Map.of("bootstrapServers", BOOTSTRAP, "topic", topic)),
        "PLAINTEXT refused without allowInsecureTransport");
    check(
        refused(
            Map.of(
                "bootstrapServers", BOOTSTRAP,
                "topic", topic,
                "kafka.security.protocol", "SASL_PLAINTEXT",
                "kafka.sasl.mechanism", "PLAIN",
                "kafka.sasl.jaas.config", security.get("sasl.jaas.config"))),
        "SASL_PLAINTEXT refused");
    check(
        refused(Map.of("bootstrapServers", BOOTSTRAP, "topic", topic, "kafka.security.protocol", "SSL")),
        "SSL without a client keystore refused");
    check(
        refused(Map.of("topic", topic, "kafka.security.protocol", "SASL_SSL")),
        "missing bootstrapServers refused");

    // 4. A rejected credential ends in a delivery failure within the timeout, not a hang.
    Map<String, String> badPassword = new LinkedHashMap<>();
    badPassword.put("bootstrapServers", BOOTSTRAP);
    badPassword.put("topic", topic);
    badPassword.put("deliveryTimeoutMs", "8000");
    badPassword.put("requestTimeoutMs", "4000");
    badPassword.put("maxBlockMs", "4000");
    security.forEach((k, v) -> badPassword.put("kafka." + k, v));
    badPassword.put(
        "kafka.sasl.jaas.config",
        "org.apache.kafka.common.security.plain.PlainLoginModule required "
            + "username=\"gravitino\" password=\"wrong\";");
    KafkaTransport transport = new KafkaTransport();
    transport.init(badPassword);
    long started = System.nanoTime();
    boolean failedCleanly = false;
    try {
      transport.send(probeEvent(), "{}".getBytes(StandardCharsets.UTF_8));
    } catch (EventTransport.TransportException e) {
      failedCleanly = true;
      System.out.println("  bad password: " + e.getMessage() + " / " + rootCause(e));
    } finally {
      transport.close();
    }
    long elapsedMs = Duration.ofNanos(System.nanoTime() - started).toMillis();
    check(failedCleanly, "bad password raises a delivery failure");
    check(elapsedMs < 15000, "failure arrives within the timeout (" + elapsedMs + " ms)");

    // 5. Credentials read from a file through Kafka's FileConfigProvider, as from a mounted
    //    Secret, so the password never appears in the server configuration.
    java.nio.file.Path credentials = java.nio.file.Files.createTempFile("kafka-credentials", ".properties");
    java.nio.file.Files.writeString(
        credentials,
        "jaas=org.apache.kafka.common.security.plain.PlainLoginModule required "
            + "username=\"gravitino\" password=\"gravitino-secret\";\n"
            + "truststore_password=trustpass\n");
    Map<String, String> fromFile = new LinkedHashMap<>();
    fromFile.put("bootstrapServers", BOOTSTRAP);
    fromFile.put("topic", topic);
    security.forEach((k, v) -> fromFile.put("kafka." + k, v));
    fromFile.put("kafka.config.providers", "file");
    fromFile.put(
        "kafka.config.providers.file.class",
        "org.apache.kafka.common.config.provider.FileConfigProvider");
    fromFile.put("kafka.sasl.jaas.config", "${file:" + credentials + ":jaas}");
    fromFile.put("kafka.ssl.truststore.password", "${file:" + credentials + ":truststore_password}");
    KafkaTransport fileTransport = new KafkaTransport();
    boolean sentWithFileCredentials = false;
    try {
      fileTransport.init(fromFile);
      fileTransport.send(probeEvent(), "{}".getBytes(StandardCharsets.UTF_8));
      sentWithFileCredentials = true;
    } catch (Exception e) {
      System.out.println("  file credentials: " + rootCause(e));
    } finally {
      fileTransport.close();
    }
    check(sentWithFileCredentials, "SASL credentials and truststore password read from a file through FileConfigProvider");

    System.out.println(failures == 0 ? "\nALL CHECKS PASSED" : "\n" + failures + " CHECK(S) FAILED");
    System.exit(failures == 0 ? 0 : 1);
  }

  private static void createTopic(String topic, Map<String, String> security) throws Exception {
    Properties p = new Properties();
    p.put("bootstrap.servers", BOOTSTRAP);
    p.putAll(security);
    try (Admin admin = Admin.create(p)) {
      admin.createTopics(Collections.singleton(new NewTopic(topic, 3, (short) 1))).all().get();
    }
  }

  private static List<ConsumerRecord<String, byte[]>> consume(
      String topic, Map<String, String> security, int expected) {
    Properties p = new Properties();
    p.put("bootstrap.servers", BOOTSTRAP);
    p.put("group.id", "smoke-" + UUID.randomUUID());
    p.put("auto.offset.reset", "earliest");
    p.put("key.deserializer", StringDeserializer.class.getName());
    p.put("value.deserializer", ByteArrayDeserializer.class.getName());
    p.putAll(security);
    List<ConsumerRecord<String, byte[]>> out = new ArrayList<>();
    try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(p)) {
      consumer.subscribe(Collections.singleton(topic));
      long deadline = System.currentTimeMillis() + 30000;
      while (out.size() < expected && System.currentTimeMillis() < deadline) {
        consumer.poll(Duration.ofMillis(500)).forEach(out::add);
      }
    }
    out.sort((a, b) -> Long.compare(a.offset(), b.offset()));
    return out;
  }

  private static boolean refused(Map<String, String> props) {
    KafkaTransport t = new KafkaTransport();
    try {
      t.init(props);
      t.close();
      return false;
    } catch (IllegalArgumentException e) {
      System.out.println("  refused: " + e.getMessage());
      return true;
    }
  }

  private static String header(ConsumerRecord<String, byte[]> r, String name) {
    Header h = r.headers().lastHeader(name);
    return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
  }

  private static String rootCause(Throwable t) {
    Throwable c = t;
    while (c.getCause() != null) {
      c = c.getCause();
    }
    return c.getClass().getSimpleName() + ": " + c.getMessage();
  }

  private static CatalogEvent probeEvent() {
    return CatalogEvent.builder()
        .id(UUID.randomUUID().toString())
        .source("https://gravitino.test/kafka-smoke")
        .type("org.apache.gravitino.table.created")
        .time(Instant.now())
        .subject("enterprise.analytics.sales.probe")
        .sequence(1)
        .build();
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
    AuditInfo audit =
        AuditInfo.builder()
            .withCreator("analytics-eng")
            .withCreateTime(Instant.parse("2026-02-04T09:12:44Z"))
            .withLastModifier("dana.okafor@example.com")
            .withLastModifiedTime(Instant.parse("2026-07-13T18:42:11Z"))
            .build();
    return new TableInfo(
        "daily_orders", cols, "Daily order totals by customer", props, null, null, null, null, audit);
  }

  private static void check(boolean ok, String what) {
    System.out.println((ok ? "  PASS  " : "  FAIL  ") + what);
    if (!ok) {
      failures++;
    }
  }
}
