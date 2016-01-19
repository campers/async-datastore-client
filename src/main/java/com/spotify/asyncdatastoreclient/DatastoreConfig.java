/*
 * Copyright (c) 2011-2015 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.spotify.asyncdatastoreclient;

import com.google.api.client.auth.oauth2.Credential;

import static com.google.common.base.MoreObjects.firstNonNull;

/**
 * Datastore configuration class used to initialise {@code Datastore}.
 * <p>
 * Use {@code DatastoreConfig.builder()} build a config object by supplying
 * options such as {@code connectTimeout()} and {@code dataset()}.
 * <p>
 * Defaults are assigned for any options not provided.
 */
public class DatastoreConfig {

  private static final int DEFAULT_CONNECT_TIMEOUT = 5000;
  private static final int DEFAULT_MAX_CONNECTIONS = -1;
  private static final int DEFAULT_REQUEST_TIMEOUTS = 5000;
  private static final int DEFAULT_REQUEST_RETRIES = 5;
  private static final String DEFAULT_HOST = "https://www.googleapis.com";
  private static final String DEFAULT_VERSION = "v1beta2";

  private int connectTimeout;
  private int maxConnections;
  private int requestTimeout;
  private int requestRetry;
  private final Credential credential;
  private final String dataset;
  private final String namespace;
  private final String host;
  private final String version;

  private DatastoreConfig(final int connectTimeout,
                          final int maxConnections,
                          final int requestTimeout,
                          final int requestRetry,
                          final Credential credential,
                          final String dataset,
                          final String namespace,
                          final String host,
                          final String version) {
    this.connectTimeout = firstNonNull(connectTimeout, DEFAULT_CONNECT_TIMEOUT);
    this.maxConnections = firstNonNull(maxConnections, DEFAULT_MAX_CONNECTIONS);
    this.requestTimeout = firstNonNull(requestTimeout, DEFAULT_REQUEST_TIMEOUTS);
    this.requestRetry = firstNonNull(requestRetry, DEFAULT_REQUEST_RETRIES);
    this.credential = credential;
    this.dataset = dataset;
    this.namespace = namespace;
    this.host = firstNonNull(host, DEFAULT_HOST);
    this.version = firstNonNull(version, DEFAULT_VERSION);
  }

  public static class Builder {
    private int connectTimeout;
    private int maxConnections;
    private int requestTimeout;
    private int requestRetry;
    private Credential credential;
    private String dataset;
    private String namespace;
    private String host;
    private String version;

    private Builder() {}

    /**
     * Creates a new {@code DatastoreConfig}.
     *
     * @return an immutable config.
     */
    public DatastoreConfig build() {
      return new DatastoreConfig(connectTimeout,
                                 maxConnections,
                                 requestTimeout,
                                 requestRetry,
                                 credential,
                                 dataset,
                                 namespace,
                                 host,
                                 version);
    }

    /**
     * Set the maximum time in milliseconds the client will can wait
     * when connecting to a remote host.
     *
     * @param connectTimeout the maximum time in milliseconds.
     * @return this config builder.
     */
    public Builder connectTimeout(final int connectTimeout) {
      this.connectTimeout = connectTimeout;
      return this;
    }

    /**
     * Set the maximum number of connections client will handle.
     *
     * @param maxConnections the maximum number of connections.
     * @return this config builder.
     */
    public Builder maxConnections(final int maxConnections) {
      this.maxConnections = maxConnections;
      return this;
    }

    /**
     * Set the maximum time in milliseconds the client waits until
     * the response is completed.
     *
     * @param requestTimeout the maximum time in milliseconds.
     * @return this config builder.
     */
    public Builder requestTimeout(final int requestTimeout) {
      this.requestTimeout = requestTimeout;
      return this;
    }

    /**
     * Set the number of times a request will be retried when an
     * {@link java.io.IOException} occurs because of a Network exception.
     *
     * @param requestRetry the number of times a request will be retried.
     * @return this config builder.
     */
    public Builder requestRetry(final int requestRetry) {
      this.requestRetry = requestRetry;
      return this;
    }

    /**
     * Set Datastore credentials to ues when requesting an access token.
     * <p>
     * Credentials can be generated by calling
     * {@code DatastoreHelper.getComputeEngineCredential} or
     * {@code DatastoreHelper.getServiceAccountCredential}
     *
     * @param credential the credentials used to authenticate.
     * @return this config builder.
     */
    public Builder credential(final Credential credential) {
      this.credential = credential;
      return this;
    }

    /**
     * Set dataset id to use when querying Datastore.
     *
     * @param dataset the dataset id.
     * @return this config builder.
     */
    public Builder dataset(final String dataset) {
      this.dataset = dataset;
      return this;
    }

    /**
     * An optional namespace may be specified to further partition data in
     * your dataset.
     *
     * @param namespace the namespace.
     * @return this config builder.
     */
    public Builder namespace(final String namespace) {
      this.namespace = namespace;
      return this;
    }

    /**
     * The Datastore host to connect to. By default, this is the Google
     * Datastore provider, however you may run a local Developer Server.
     *
     * @param host the host to connect to.
     * @return this config builder.
     */
    public Builder host(final String host) {
      this.host = host;
      return this;
    }

    /**
     * The Datastore API version to use.
     *
     * @param version the version to use.
     * @return this config builder.
     */
    public Builder version(final String version) {
      this.version = version;
      return this;
    }
  }

  public static DatastoreConfig.Builder builder() {
    return new DatastoreConfig.Builder();
  }

  public int getConnectTimeout() {
    return connectTimeout;
  }

  public int getMaxConnections() {
    return maxConnections;
  }

  public int getRequestTimeout() {
    return requestTimeout;
  }

  public int getRequestRetry() {
    return requestRetry;
  }

  public Credential getCredential() {
    return credential;
  }

  public String getDataset() {
    return dataset;
  }

  public String getNamespace() {
    return namespace;
  }

  public String getHost() {
    return host;
  }

  public String getVersion() {
    return version;
  }
}

