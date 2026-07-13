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
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

/**
 * Authenticates outbound event delivery.
 *
 * <p>Required, always. {@link #fromConfig} throws if no scheme is configured. There is deliberately
 * no {@code none} mode and no "just for testing" escape hatch, because a testing escape hatch is
 * how an unauthenticated push into a governance system reaches production.
 *
 * <p>Supported schemes:
 *
 * <ul>
 *   <li>{@code oauth2} - client credentials grant, token cached and refreshed ahead of expiry.
 *       The default, and the one to reach for against an enterprise IdP such as Entra or Keycloak.
 *   <li>{@code mtls} - client certificate. Common in bank internal networks where the service mesh
 *       already does mutual TLS.
 *   <li>{@code bearer} - a static token. Present because some in-house systems accept nothing else.
 *       Weakest of the three.
 * </ul>
 */
public abstract class AuthProvider implements Closeable {

  /** Applies credentials to an outbound request. Called per request, so must be cheap and thread-safe. */
  abstract void applyTo(HttpRequest.Builder request);

  /** A custom SSL context, when the scheme needs one. Empty otherwise. */
  Optional<SSLContext> sslContext() {
    return Optional.empty();
  }

  /** Describes the configured scheme, for logging. Never emits secrets. */
  abstract String describe();

  @Override
  public void close() {}

  /**
   * Builds a provider from configuration.
   *
   * @throws IllegalArgumentException if {@code auth.type} is absent or unrecognised. Fail at
   *     startup, not at first event.
   */
  static AuthProvider fromConfig(Map<String, String> props) {
    String type = props.get("auth.type");
    if (type == null || type.trim().isEmpty()) {
      throw new IllegalArgumentException(
          "auth.type is required. Supported: oauth2, mtls, bearer. "
              + "Unauthenticated event delivery is not supported.");
    }

    switch (type.trim().toLowerCase(Locale.ROOT)) {
      case "oauth2":
        return new OAuth2ClientCredentials(props);
      case "mtls":
        return new MutualTls(props);
      case "bearer":
        return new StaticBearer(props);
      default:
        throw new IllegalArgumentException(
            "unsupported auth.type: " + type + ". Supported: oauth2, mtls, bearer.");
    }
  }

  /** Extra headers the operator wants on every request, for example a tenant or routing header. */
  static Map<String, String> staticHeaders(Map<String, String> props) {
    Map<String, String> headers = new LinkedHashMap<>();
    String prefix = "header.";
    props.forEach(
        (k, v) -> {
          if (k.startsWith(prefix) && k.length() > prefix.length()) {
            headers.put(k.substring(prefix.length()), v);
          }
        });
    return Collections.unmodifiableMap(headers);
  }

  // -------------------------------------------------------------------------
  // OAuth2 client credentials
  // -------------------------------------------------------------------------

  /** Client credentials grant, with a cached token refreshed ahead of expiry. */
  private static final class OAuth2ClientCredentials extends AuthProvider {
    private final URI tokenEndpoint;
    private final String clientId;
    private final String clientSecret;
    private final String scope;
    private final HttpClient tokenClient;

    private volatile String token;
    private volatile Instant expiresAt = Instant.EPOCH;

    /** Refresh this far ahead of stated expiry, so an in-flight request never carries a dead token. */
    private static final Duration REFRESH_SKEW = Duration.ofSeconds(60);

    OAuth2ClientCredentials(Map<String, String> p) {
      this.tokenEndpoint = URI.create(required(p, "auth.oauth2.tokenEndpoint"));
      this.clientId = required(p, "auth.oauth2.clientId");
      this.clientSecret = required(p, "auth.oauth2.clientSecret");
      this.scope = p.getOrDefault("auth.oauth2.scope", "");
      this.tokenClient =
          HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    }

    @Override
    void applyTo(HttpRequest.Builder request) {
      request.header("Authorization", "Bearer " + currentToken());
    }

    private String currentToken() {
      if (token != null && Instant.now().isBefore(expiresAt.minus(REFRESH_SKEW))) {
        return token;
      }
      synchronized (this) {
        if (token != null && Instant.now().isBefore(expiresAt.minus(REFRESH_SKEW))) {
          return token;
        }
        refresh();
        return token;
      }
    }

    private void refresh() {
      String form = "grant_type=client_credentials";
      if (!scope.isEmpty()) {
        form += "&scope=" + java.net.URLEncoder.encode(scope, StandardCharsets.UTF_8);
      }

      String basic =
          Base64.getEncoder()
              .encodeToString(
                  (clientId + ":" + clientSecret).getBytes(StandardCharsets.UTF_8));

      HttpRequest req =
          HttpRequest.newBuilder(tokenEndpoint)
              .timeout(Duration.ofSeconds(10))
              .header("Content-Type", "application/x-www-form-urlencoded")
              .header("Authorization", "Basic " + basic)
              .POST(HttpRequest.BodyPublishers.ofString(form))
              .build();

      try {
        HttpResponse<String> resp =
            tokenClient.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200) {
          throw new IllegalStateException(
              "token endpoint returned HTTP " + resp.statusCode());
        }
        Map<String, Object> body =
            new com.fasterxml.jackson.databind.ObjectMapper()
                .readValue(resp.body(), new com.fasterxml.jackson.core.type.TypeReference<>() {});

        this.token = (String) body.get("access_token");
        long expiresIn =
            body.get("expires_in") instanceof Number
                ? ((Number) body.get("expires_in")).longValue()
                : 3600L;
        this.expiresAt = Instant.now().plusSeconds(expiresIn);

      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("interrupted acquiring OAuth2 token", e);
      } catch (Exception e) {
        throw new IllegalStateException("failed to acquire OAuth2 token", e);
      }
    }

    @Override
    String describe() {
      return "oauth2(clientId=" + clientId + ", tokenEndpoint=" + tokenEndpoint + ")";
    }
  }

  // -------------------------------------------------------------------------
  // mTLS
  // -------------------------------------------------------------------------

  /** Client certificate authentication. */
  private static final class MutualTls extends AuthProvider {
    private final SSLContext ctx;
    private final String keyStorePath;

    MutualTls(Map<String, String> p) {
      this.keyStorePath = required(p, "auth.mtls.keyStorePath");
      String keyStorePassword = required(p, "auth.mtls.keyStorePassword");
      String trustStorePath = p.get("auth.mtls.trustStorePath");
      String trustStorePassword = p.get("auth.mtls.trustStorePassword");

      try {
        KeyStore ks = KeyStore.getInstance("PKCS12");
        try (var in = Files.newInputStream(Paths.get(keyStorePath))) {
          ks.load(in, keyStorePassword.toCharArray());
        }
        KeyManagerFactory kmf =
            KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(ks, keyStorePassword.toCharArray());

        TrustManagerFactory tmf = null;
        if (trustStorePath != null) {
          KeyStore ts = KeyStore.getInstance("PKCS12");
          try (var in = Files.newInputStream(Paths.get(trustStorePath))) {
            ts.load(
                in,
                trustStorePassword == null ? null : trustStorePassword.toCharArray());
          }
          tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
          tmf.init(ts);
        }

        SSLContext c = SSLContext.getInstance("TLS");
        c.init(kmf.getKeyManagers(), tmf == null ? null : tmf.getTrustManagers(), null);
        this.ctx = c;

      } catch (Exception e) {
        throw new IllegalArgumentException("failed to build mTLS context", e);
      }
    }

    @Override
    void applyTo(HttpRequest.Builder request) {
      // Nothing to add. The certificate is presented during the TLS handshake.
    }

    @Override
    Optional<SSLContext> sslContext() {
      return Optional.of(ctx);
    }

    @Override
    String describe() {
      return "mtls(keyStore=" + keyStorePath + ")";
    }
  }

  // -------------------------------------------------------------------------
  // Static bearer
  // -------------------------------------------------------------------------

  /** A static bearer token. Supported because some in-house systems accept nothing else. */
  private static final class StaticBearer extends AuthProvider {
    private final String token;

    StaticBearer(Map<String, String> p) {
      this.token = required(p, "auth.bearer.token");
    }

    @Override
    void applyTo(HttpRequest.Builder request) {
      request.header("Authorization", "Bearer " + token);
    }

    @Override
    String describe() {
      return "bearer(static)";
    }
  }

  private static String required(Map<String, String> p, String key) {
    String v = p.get(key);
    if (v == null || v.trim().isEmpty()) {
      throw new IllegalArgumentException("missing required property: " + key);
    }
    return v.trim();
  }
}
