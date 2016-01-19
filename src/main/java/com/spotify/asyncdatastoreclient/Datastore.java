/*
 * Copyright (c) 2011-2015 Spotify AB
 *
 * Copyright (c) 2016 Daniel Campagnoli, Software Engineers Toolbox
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.spotify.asyncdatastoreclient;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import co.paralleluniverse.fibers.httpasyncclient.FiberCloseableHttpAsyncClient;
import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.http.protobuf.ProtoHttpContent;
import com.google.api.services.datastore.DatastoreV1;
import com.google.common.base.Throwables;
import com.google.protobuf.ByteString;

/**
 * The Datastore class encapsulates the Cloud Datastore API and handles calling the datastore
 * backend.
 * <p>
 * To create a Datastore object, call the static method {@code Datastore.create()} passing
 * configuration. A scheduled task will begin that automatically refreshes the API access token for
 * you.
 * <p>
 * Call {@code close()} to perform all necessary clean up.
 */
public class Datastore implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(Datastore.class);

  public static final String VERSION = "1.0.0";

  public static final String USER_AGENT = "Datastore-Java-Client/" + VERSION + " (gzip)";

  private final DatastoreConfig config;

  private final CloseableHttpAsyncClient client;

  private final String prefixUri;

  private ScheduledExecutorService executor;

  private volatile String accessToken;

  public enum IsolationLevel {
    SNAPSHOT,
    SERIALIZABLE
  }

  private Datastore(final DatastoreConfig config) {
    this.config = config;

    // TODO implement ning config which doesn't exist on apache http client
    //    .setCompressionEnforced(true)

    RequestConfig requestConfig = RequestConfig.custom()
        .setConnectTimeout(config.getConnectTimeout())
        .setConnectionRequestTimeout(config.getRequestTimeout()).build();

    client = FiberCloseableHttpAsyncClient.wrap(HttpAsyncClients.custom()
        .setDefaultRequestConfig(requestConfig).setMaxConnPerRoute(config.getMaxConnections())
        .setMaxConnTotal(config.getMaxConnections()).build());

    client.start();
    prefixUri = String.format("%s/datastore/%s/datasets/%s/", config.getHost(),
        config.getVersion(), config.getDataset());
    executor = Executors.newSingleThreadScheduledExecutor();

    if (config.getCredential() != null) {
      // block while retrieving an access token for the first time
      refreshAccessToken();

      // wake up every 10 seconds to check if access token has expired
      executor.scheduleAtFixedRate(this::refreshAccessToken, 10, 10, TimeUnit.SECONDS);
    }
  }

  public static Datastore create(final DatastoreConfig config) {
    return new Datastore(config);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() throws IOException {
    executor.shutdown();
    client.close();
  }

  private void refreshAccessToken() {
    final Credential credential = config.getCredential();
    final Long expiresIn = credential.getExpiresInSeconds();

    // trigger refresh if token is about to expire
    if (credential.getAccessToken() == null || expiresIn != null && expiresIn.longValue() <= 60) {
      try {
        credential.refreshToken();
        final String accessToken = credential.getAccessToken();
        if (accessToken != null) {
          this.accessToken = accessToken;
        }
      } catch (final IOException e) {
        log.error("Storage exception", Throwables.getRootCause(e));
      }
    }
  }

  private void checkSuccessful(final HttpResponse httpResponse) throws DatastoreException {
    int statusCode = httpResponse.getStatusLine().getStatusCode();
    if (!(statusCode >= 200 && statusCode < 300)) {
      try {
        throw new DatastoreException(statusCode, EntityUtils.toString(httpResponse.getEntity(),
            "UTF-8"));
      } catch (IOException e) {
        throw new DatastoreException(statusCode, "Error parsing HTTP response:" + e.getMessage());
      }
    }
  }

  private HttpResponse executeRequest(final String method, final ProtoHttpContent payload)
      throws DatastoreException {
    int maxRetries = config.getRequestRetry();
    int i = 0;
    ByteArrayEntity entity = new ByteArrayEntity(payload.getMessage().toByteArray());
    while (i++ <= maxRetries) {
      try {
        HttpPost httpPost = new HttpPost(prefixUri + method);
        httpPost.addHeader("Authorization", "Bearer " + accessToken);
        httpPost.addHeader("Content-Type", "application/x-protobuf");
        httpPost.addHeader("User-Agent", USER_AGENT);
        httpPost.addHeader("Accept-Encoding", "gzip");

        httpPost.setEntity(entity);

        HttpResponse httpResponse = client.execute(httpPost, null).get();
        checkSuccessful(httpResponse);

        return httpResponse;
      } catch (Exception e) {
        if (i == maxRetries) {
          throw new DatastoreException(e);
        }
      }
    }
    // Should never reach here
    throw new DatastoreException("");
  }

  private InputStream streamResponse(final HttpResponse response) throws IOException {
    final InputStream input = response.getEntity().getContent();
    final boolean compressed = "gzip".equals(response.getFirstHeader("Content-Encoding"));
    return compressed ? new GZIPInputStream(input) : input;
  }

  /**
   * Start a new transaction.
   *
   * The returned {@code TransactionResult} contains the transaction if the request is successful.
   *
   * @return the result of the transaction request.
   * @throws DatastoreException
   */
  public TransactionResult transaction() throws DatastoreException {
    return transaction(IsolationLevel.SNAPSHOT);
  }

  /**
   * Start a new transaction with a given isolation level.
   *
   * The returned {@code TransactionResult} contains the transaction if the request is successful.
   *
   * @param isolationLevel
   *          the transaction isolation level to request.
   * @return the result of the transaction request.
   * @throws DatastoreException
   */
  public TransactionResult transaction(final IsolationLevel isolationLevel)
      throws DatastoreException {
    try {
      final DatastoreV1.BeginTransactionRequest.Builder request = DatastoreV1.BeginTransactionRequest
          .newBuilder();
      if (isolationLevel == IsolationLevel.SERIALIZABLE) {
        request.setIsolationLevel(DatastoreV1.BeginTransactionRequest.IsolationLevel.SERIALIZABLE);
      } else {
        request.setIsolationLevel(DatastoreV1.BeginTransactionRequest.IsolationLevel.SNAPSHOT);
      }
      final ProtoHttpContent payload = new ProtoHttpContent(request.build());
      HttpResponse httpResponse = executeRequest("beginTransaction", payload);

      final DatastoreV1.BeginTransactionResponse transaction = DatastoreV1.BeginTransactionResponse
          .parseFrom(streamResponse(httpResponse));
      return TransactionResult.build(transaction);
    } catch (final Exception e) {
      throw dsException(e);
    }
  }

  /**
   * Rollback a given transaction.
   *
   * You normally rollback a transaction in the event of a Datastore failure.
   *
   * @param txn
   *          the transaction.
   * @return the result of the rollback request.
   * @throws DatastoreException
   */
  public RollbackResult rollback(final TransactionResult txn) throws DatastoreException {

    try {
      final DatastoreV1.RollbackRequest.Builder request = DatastoreV1.RollbackRequest.newBuilder();
      final ByteString transaction = txn.getTransaction();
      if (transaction == null) {
        throw new DatastoreException("Invalid transaction");
      }

      final ProtoHttpContent payload = new ProtoHttpContent(request.build());
      HttpResponse httpResponse = executeRequest("rollback", payload);

      final DatastoreV1.RollbackResponse rollback = DatastoreV1.RollbackResponse
          .parseFrom(streamResponse(httpResponse));
      return RollbackResult.build(rollback);
    } catch (Exception e) {
      throw dsException(e);
    }
  }

  /**
   * Commit a given transaction.
   *
   * You normally manually commit a transaction after performing read-only operations without
   * mutations.
   *
   * @param txn
   *          the transaction.
   * @return the result of the commit request.
   * @throws DatastoreException
   */
  public MutationResult commit(final TransactionResult txn) throws DatastoreException {
    return execute((MutationStatement)null, txn);
  }

  /**
   * Execute a allocate ids statement.
   *
   * @param statement
   *          the statement to execute.
   * @return the result of the allocate ids request.
   * @throws DatastoreException
   */
  public AllocateIdsResult execute(final AllocateIds statement) throws DatastoreException {
    try {
      final DatastoreV1.AllocateIdsRequest.Builder request = DatastoreV1.AllocateIdsRequest
          .newBuilder().addAllKey(statement.getPb(config.getNamespace()));
      final ProtoHttpContent payload = new ProtoHttpContent(request.build());
      final HttpResponse httpResponse = executeRequest("allocateIds", payload);

      final DatastoreV1.AllocateIdsResponse allocate = DatastoreV1.AllocateIdsResponse
          .parseFrom(streamResponse(httpResponse));
      return AllocateIdsResult.build(allocate);

    } catch (final Exception e) {
      throw dsException(e);
    }
  }

  /**
   * Execute a keyed query statement.
   *
   * @param statement
   *          the statement to execute.
   * @return the result of the query request.
   * @throws DatastoreException
   */
  public QueryResult execute(final KeyQuery statement) throws DatastoreException {
    return execute(statement, TransactionResult.build());
  }

  /**
   * Execute a keyed query statement in a given transaction.
   *
   * @param statement
   *          the statement to execute.
   * @param txn
   *          the transaction to execute the query.
   * @return the result of the query request.
   * @throws DatastoreException
   */
  public QueryResult execute(final KeyQuery statement, final TransactionResult txn)
      throws DatastoreException {
    try {
      final HttpResponse httpResponse;
      final DatastoreV1.Key key = statement.getKey().getPb(config.getNamespace());
      final DatastoreV1.LookupRequest.Builder request = DatastoreV1.LookupRequest.newBuilder()
          .addKey(key);
      final ByteString transaction = txn.getTransaction();
      if (transaction != null) {
        request.setReadOptions(DatastoreV1.ReadOptions.newBuilder().setTransaction(transaction));
      }
      final ProtoHttpContent payload = new ProtoHttpContent(request.build());
      httpResponse = executeRequest("lookup", payload);

      final DatastoreV1.LookupResponse query = DatastoreV1.LookupResponse
          .parseFrom(streamResponse(httpResponse));
      return QueryResult.build(query);

    } catch (Exception e) {
      throw dsException(e);
    }
  }

  /**
   * Execute a mutation query statement.
   *
   * @param statement
   *          the statement to execute.
   * @return the result of the mutation request.
   * @throws DatastoreException
   */
  public MutationResult execute(final MutationStatement statement) throws DatastoreException {
    return execute(statement, TransactionResult.build());
  }

  /**
   * Execute a mutation query statement in a given transaction.
   *
   * @param statement
   *          the statement to execute.
   * @param txn
   *          the transaction to execute the query.
   * @return the result of the mutation request.
   * @throws DatastoreException
   */
  public MutationResult execute(final MutationStatement statement, final TransactionResult txn)
      throws DatastoreException {
    try {

      final HttpResponse httpResponse;
      final DatastoreV1.CommitRequest.Builder request = DatastoreV1.CommitRequest.newBuilder();
      if (statement != null) {
        request.setMutation(statement.getPb(config.getNamespace()));
      }
      final ByteString transaction = txn.getTransaction();
      if (transaction != null) {
        request.setTransaction(transaction);
      } else {
        request.setMode(DatastoreV1.CommitRequest.Mode.NON_TRANSACTIONAL);
      }
      final ProtoHttpContent payload = new ProtoHttpContent(request.build());
      httpResponse = executeRequest("commit", payload);

      final DatastoreV1.CommitResponse commit = DatastoreV1.CommitResponse
          .parseFrom(streamResponse(httpResponse));
      return MutationResult.build(commit);
    } catch (Exception e) {
      throw dsException(e);
    }
  }

  /**
   * Execute a query statement.
   *
   * @param statement
   *          the statement to execute.
   * @return the result of the query request.
   * @throws DatastoreException
   */
  public QueryResult execute(final Query statement) throws DatastoreException {
    return execute(statement, TransactionResult.build());
  }

  /**
   * Execute a query statement in a given transaction.
   *
   * @param statement
   *          the statement to execute.
   * @param txn
   *          the transaction to execute the query.
   * @return the result of the query request.
   * @throws DatastoreException
   */
  public QueryResult execute(final Query statement, final TransactionResult txn)
      throws DatastoreException {
    try {
      final DatastoreV1.RunQueryRequest.Builder request = DatastoreV1.RunQueryRequest.newBuilder()
          .setQuery(statement.getPb());
      final String namespace = config.getNamespace();
      if (namespace != null) {
        request.setPartitionId(DatastoreV1.PartitionId.newBuilder().setNamespace(namespace));
      }
      final ByteString transaction = txn.getTransaction();
      if (transaction != null) {
        request.setReadOptions(DatastoreV1.ReadOptions.newBuilder().setTransaction(transaction));
      }
      final ProtoHttpContent payload = new ProtoHttpContent(request.build());
      final HttpResponse httpResponse = executeRequest("runQuery", payload);

      final DatastoreV1.RunQueryResponse query = DatastoreV1.RunQueryResponse
          .parseFrom(streamResponse(httpResponse));
      return QueryResult.build(query);

    } catch (Exception e) {
      throw dsException(e);
    }
  }

  private static DatastoreException dsException(Exception e) {
    if (e instanceof DatastoreException) {
      return (DatastoreException)e;
    }
    return new DatastoreException(e);
  }

}
