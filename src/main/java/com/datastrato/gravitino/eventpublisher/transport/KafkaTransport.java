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

import com.datastrato.gravitino.eventpublisher.wire.CatalogEvent;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Delivers events to a Kafka topic.
 *
 * <p>The record value is the same {@code payload} bytes {@link HttpTransport} posts, a CloudEvents
 * document in structured mode, so a consumer reading the topic sees exactly what an HTTP endpoint
 * would receive. The record key is the event {@code subject}, the fully qualified name of the
 * entity, so every event for one table lands on one partition in commit order and a consumer
 * applying full-state upserts never sees an older state after a newer one.
 *
 * <p>Retries are the producer's, not ours. The producer is idempotent with {@code acks=all} and
 * retries within {@code deliveryTimeoutMs}; when that budget is spent the send fails and the event
 * is dead-lettered by the listener, the same as an HTTP delivery that exhausts its attempts.
 *
 * <p>Authentication is mandatory, as for HTTP. The connection must use {@code SASL_SSL}, or
 * {@code SSL} with a client keystore (mutual TLS). {@code PLAINTEXT} and {@code SASL_PLAINTEXT} are
 * refused unless {@code allowInsecureTransport=true}, which exists for a local broker during
 * development and must never be set in a deployed environment.
 *
 * <p>Every property under {@code kafka.} is passed to the producer with the prefix removed, so TLS
 * and SASL are configured the standard Kafka way: {@code kafka.security.protocol}, {@code
 * kafka.sasl.mechanism}, {@code kafka.sasl.jaas.config}, {@code kafka.ssl.truststore.location}, and
 * so on.
 */
public final class KafkaTransport implements EventTransport {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaTransport.class);

  private static final String KAFKA_PREFIX = "kafka.";
  private static final String CONTENT_TYPE = "application/cloudevents+json; charset=UTF-8";

  private String topic;
  private long sendTimeoutMs;
  private Producer<String, byte[]> producer;

  @Override
  public void init(Map<String, String> props) {
    String bootstrapServers = require(props, "bootstrapServers");
    this.topic = require(props, "topic");

    long deliveryTimeoutMs = Long.parseLong(props.getOrDefault("deliveryTimeoutMs", "30000"));
    long requestTimeoutMs = Long.parseLong(props.getOrDefault("requestTimeoutMs", "10000"));
    long maxBlockMs = Long.parseLong(props.getOrDefault("maxBlockMs", "5000"));
    // The caller waits no longer than the producer can spend on one record, plus the time send()
    // may block fetching metadata, so one unreachable broker cannot stall the dispatcher.
    this.sendTimeoutMs = deliveryTimeoutMs + maxBlockMs;

    Properties config = new Properties();
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    config.put(ProducerConfig.CLIENT_ID_CONFIG, props.getOrDefault("clientId", "gravitino-event-publisher"));
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    config.put(ProducerConfig.ACKS_CONFIG, "all");
    config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, String.valueOf(deliveryTimeoutMs));
    config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(requestTimeoutMs));
    config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, String.valueOf(maxBlockMs));
    // gzip is built into the JDK. The jar does not bundle the native lz4, snappy or zstd codecs.
    config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");

    props.forEach(
        (key, value) -> {
          if (key.startsWith(KAFKA_PREFIX)) {
            config.put(key.substring(KAFKA_PREFIX.length()), value);
          }
        });

    requireAuthenticatedConnection(config, props);

    // The producer loads serializers and login modules through the thread context class loader.
    // Gravitino calls init on its own thread, so point the context at this plugin's loader for the
    // duration of construction.
    ClassLoader previous = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(KafkaTransport.class.getClassLoader());
      this.producer = new KafkaProducer<>(config);
    } finally {
      Thread.currentThread().setContextClassLoader(previous);
    }

    LOG.info(
        "Kafka transport initialised: bootstrapServers={}, topic={}, securityProtocol={}",
        bootstrapServers,
        topic,
        config.getProperty("security.protocol", "PLAINTEXT"));
  }

  @Override
  public void send(CatalogEvent event, byte[] payload) throws TransportException {
    ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, event.subject(), payload);
    record.headers().add("content-type", CONTENT_TYPE.getBytes(StandardCharsets.UTF_8));

    try {
      Future<RecordMetadata> result = producer.send(record);
      result.get(sendTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new TransportException("interrupted delivering event " + event.id(), e);
    } catch (ExecutionException e) {
      throw new TransportException(
          "delivery failed for event " + event.id() + " to topic " + topic, e.getCause());
    } catch (TimeoutException e) {
      throw new TransportException(
          "delivery timed out for event " + event.id() + " to topic " + topic, e);
    } catch (RuntimeException e) {
      // send() throws synchronously when metadata cannot be fetched within maxBlockMs.
      throw new TransportException(
          "delivery failed for event " + event.id() + " to topic " + topic, e);
    }
  }

  @Override
  public String name() {
    return "kafka";
  }

  @Override
  public void close() {
    if (producer != null) {
      producer.close(java.time.Duration.ofSeconds(10));
    }
  }

  /**
   * Refuses to start without an authenticated connection.
   *
   * <p>{@code SASL_SSL} authenticates through SASL over TLS. {@code SSL} authenticates only when the
   * client presents a certificate, so it must come with a keystore.
   */
  private static void requireAuthenticatedConnection(Properties config, Map<String, String> props) {
    if (Boolean.parseBoolean(props.getOrDefault("allowInsecureTransport", "false"))) {
      LOG.warn(
          "Kafka transport is running with allowInsecureTransport=true. Use this only against a "
              + "local broker in development, never in a deployed environment.");
      return;
    }

    String protocol =
        config.getProperty("security.protocol", "PLAINTEXT").trim().toUpperCase(Locale.ROOT);
    switch (protocol) {
      case "SASL_SSL":
        if (config.getProperty("sasl.jaas.config", "").trim().isEmpty()) {
          throw new IllegalArgumentException(
              "kafka.security.protocol=SASL_SSL requires kafka.sasl.jaas.config");
        }
        return;
      case "SSL":
        if (config.getProperty("ssl.keystore.location", "").trim().isEmpty()) {
          throw new IllegalArgumentException(
              "kafka.security.protocol=SSL authenticates the client only with a certificate. "
                  + "Set kafka.ssl.keystore.location, or use SASL_SSL.");
        }
        return;
      default:
        throw new IllegalArgumentException(
            "kafka.security.protocol must be SASL_SSL, or SSL with a client keystore. Got "
                + protocol
                + ". Set allowInsecureTransport=true only for a local broker in development.");
    }
  }

  private static String require(Map<String, String> props, String key) {
    String v = props.get(key);
    if (v == null || v.trim().isEmpty()) {
      throw new IllegalArgumentException("missing required property: " + key);
    }
    return v.trim();
  }
}
