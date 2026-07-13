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

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import com.datastrato.gravitino.eventpublisher.wire.CatalogEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Delivers events over HTTP POST.
 *
 * <p>The first transport because it is the one most in-house catalogs can already receive, not
 * because it is architecturally privileged. A Kafka transport is the same class with a producer in
 * place of the {@link HttpClient} and the same {@code payload} bytes on the wire.
 *
 * <p>Authentication is mandatory. There is no unauthenticated mode, not even for local testing.
 * Omitting it here would mean the production deployment omits it too, and no bank will accept an
 * unauthenticated push into a governance system.
 */
public final class HttpTransport implements EventTransport {

  private static final Logger LOG = LoggerFactory.getLogger(HttpTransport.class);

  private URI endpoint;
  private HttpClient client;
  private AuthProvider auth;
  private Map<String, String> staticHeaders;

  private Duration requestTimeout;
  private int maxAttempts;
  private Duration initialBackoff;
  private Duration maxBackoff;

  @Override
  public void init(Map<String, String> props) {
    String url = require(props, "url");
    this.endpoint = URI.create(url);

    if (!"https".equalsIgnoreCase(endpoint.getScheme())
        && !Boolean.parseBoolean(props.getOrDefault("allowInsecureTransport", "false"))) {
      throw new IllegalArgumentException(
          "url must be https. Set allowInsecureTransport=true only for a loopback endpoint "
              + "in local development, never in a deployed environment.");
    }

    // Auth is required, and the failure is at startup rather than at first event.
    this.auth = AuthProvider.fromConfig(props);
    this.staticHeaders = AuthProvider.staticHeaders(props);

    this.requestTimeout =
        Duration.ofMillis(Long.parseLong(props.getOrDefault("requestTimeoutMs", "10000")));
    this.maxAttempts = Integer.parseInt(props.getOrDefault("maxAttempts", "4"));
    this.initialBackoff =
        Duration.ofMillis(Long.parseLong(props.getOrDefault("initialBackoffMs", "250")));
    this.maxBackoff = Duration.ofMillis(Long.parseLong(props.getOrDefault("maxBackoffMs", "5000")));

    HttpClient.Builder b =
        HttpClient.newBuilder()
            .connectTimeout(
                Duration.ofMillis(Long.parseLong(props.getOrDefault("connectTimeoutMs", "5000"))))
            .followRedirects(HttpClient.Redirect.NEVER);

    auth.sslContext().ifPresent(b::sslContext);
    this.client = b.build();

    LOG.info(
        "HTTP transport initialised: endpoint={}, auth={}, maxAttempts={}",
        endpoint,
        auth.describe(),
        maxAttempts);
  }

  @Override
  public void send(CatalogEvent event, byte[] payload) throws TransportException {
    Throwable last = null;
    Duration backoff = initialBackoff;

    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        HttpRequest.Builder req =
            HttpRequest.newBuilder(endpoint)
                .timeout(requestTimeout)
                .header("Content-Type", "application/cloudevents+json; charset=utf-8")
                // Idempotency key. The event id is stable across retries, so a consumer that
                // honours it will not double-apply a redelivery.
                .header("Idempotency-Key", event.id())
                .POST(HttpRequest.BodyPublishers.ofByteArray(payload));

        staticHeaders.forEach(req::header);
        auth.applyTo(req);

        HttpResponse<String> resp = client.send(req.build(), HttpResponse.BodyHandlers.ofString());
        int status = resp.statusCode();

        if (status >= 200 && status < 300) {
          return;
        }

        // 4xx other than 408 and 429 will not succeed on retry. Fail immediately and dead-letter,
        // rather than burning the retry budget on a request the server will keep rejecting.
        if (status >= 400 && status < 500 && status != 408 && status != 429) {
          throw new TransportException(
              "endpoint rejected event " + event.id() + " with HTTP " + status + ": " + body(resp));
        }

        last = new IOException("HTTP " + status + ": " + body(resp));

      } catch (TransportException e) {
        throw e;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new TransportException("interrupted delivering event " + event.id(), e);
      } catch (Exception e) {
        last = e;
      }

      if (attempt < maxAttempts) {
        sleep(jitter(backoff));
        backoff = Duration.ofMillis(Math.min(backoff.toMillis() * 2, maxBackoff.toMillis()));
      }
    }

    throw new TransportException(
        "delivery failed for event " + event.id() + " after " + maxAttempts + " attempts",
        last);
  }

  @Override
  public String name() {
    return "http";
  }

  @Override
  public void close() {
    if (auth != null) {
      auth.close();
    }
  }

  private static String body(HttpResponse<String> resp) {
    String b = resp.body();
    if (b == null) {
      return "";
    }
    return b.length() > 512 ? b.substring(0, 512) + "..." : b;
  }

  /** Full jitter, to stop every event in a burst retrying in lockstep against a struggling endpoint. */
  private static Duration jitter(Duration base) {
    long ms = base.toMillis();
    return Duration.ofMillis(ms <= 0 ? 0 : ThreadLocalRandom.current().nextLong(ms + 1));
  }

  private static void sleep(Duration d) {
    try {
      Thread.sleep(d.toMillis());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
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
