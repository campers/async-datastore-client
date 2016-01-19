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

import static java.util.concurrent.CompletableFuture.completedFuture;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import org.apache.http.HttpResponse;
import org.apache.http.ParseException;
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
import com.google.common.util.concurrent.Futures;
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
    if (credential.getAccessToken() == null || expiresIn != null && expiresIn <= 60) {
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
    if (statusCode >= 200 && statusCode < 300) {
      exceptionally(httpResponse);
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
          exceptionally(e);
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
   */
  public TransactionResult transaction() throws DatastoreException {
    return Futures.get(transactionAsync(IsolationLevel.SNAPSHOT), DatastoreException.class);
  }

  /**
   * Start a new transaction with a given isolation level.
   *
   * The returned {@code TransactionResult} contains the transaction if the request is successful.
   *
   * @param isolationLevel
   *          the transaction isolation level to request.
   * @return the result of the transaction request.
   */
  public TransactionResult transaction(final IsolationLevel isolationLevel)
      throws DatastoreException {
    return Futures.get(transactionAsync(isolationLevel), DatastoreException.class);
  }

  /**
   * Start a new transaction.
   *
   * The returned {@code TransactionResult} contains the transaction if the request is successful.
   *
   * @return the result of the transaction request.
   */
  public CompletableFuture<TransactionResult> transactionAsync() {
    return transactionAsync(IsolationLevel.SNAPSHOT);
  }

  /**
   * Start a new transaction.
   *
   * The returned {@code TransactionResult} contains the transaction if the request is successful.
   *
   * @param isolationLevel
   *          the transaction isolation level to request.
   * @return the result of the transaction request.
   */
  public CompletableFuture<TransactionResult> transactionAsync(final IsolationLevel isolationLevel) {
    final HttpResponse httpResponse;
    try {
      final DatastoreV1.BeginTransactionRequest.Builder request = DatastoreV1.BeginTransactionRequest
          .newBuilder();
      if (isolationLevel == IsolationLevel.SERIALIZABLE) {
        request.setIsolationLevel(DatastoreV1.BeginTransactionRequest.IsolationLevel.SERIALIZABLE);
      } else {
        request.setIsolationLevel(DatastoreV1.BeginTransactionRequest.IsolationLevel.SNAPSHOT);
      }
      final ProtoHttpContent payload = new ProtoHttpContent(request.build());
      httpResponse = executeRequest("beginTransaction", payload);

      final DatastoreV1.BeginTransactionResponse transaction = DatastoreV1.BeginTransactionResponse
          .parseFrom(streamResponse(httpResponse));
      return completedFuture(TransactionResult.build(transaction));
    } catch (final Exception e) {
      return exceptionally(e);
    }
  }

  /**
   * Rollback a given transaction.
   *
   * You normally rollback a transaction in the event of d Datastore failure.
   *
   * @param txn
   *          the transaction.
   * @return the result of the rollback request.
   */
  public RollbackResult rollback(final TransactionResult txn) throws DatastoreException {
    return Futures.get(rollbackAsync(completedFuture(txn)), DatastoreException.class);
  }

  /**
   * Rollback a given transaction.
   *
   * You normally rollback a transaction in the event of d Datastore failure.
   *
   * @param txn
   *          the transaction.
   * @return the result of the rollback request.
   */
  public CompletableFuture<RollbackResult> rollbackAsync(
      final CompletableFuture<TransactionResult> txn) {

    try {
      final DatastoreV1.RollbackRequest.Builder request = DatastoreV1.RollbackRequest.newBuilder();
      final ByteString transaction = txn.get().getTransaction();
      if (transaction == null) {
        exceptionally("Invalid transaction.");
      }
      final ProtoHttpContent payload = new ProtoHttpContent(request.build());
      HttpResponse httpResponse = executeRequest("rollback", payload);

      final DatastoreV1.RollbackResponse rollback = DatastoreV1.RollbackResponse
          .parseFrom(streamResponse(httpResponse));
      return completedFuture(RollbackResult.build(rollback));
    } catch (Exception e) {
      return exceptionally(e);
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
   */
  public MutationResult commit(final TransactionResult txn) throws DatastoreException {
    return Futures.get(executeAsync((MutationStatement)null, completedFuture(txn)),
        DatastoreException.class);
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
   */
  public CompletableFuture<MutationResult> commitAsync(
      final CompletableFuture<TransactionResult> txn) {
    return executeAsync((MutationStatement)null, txn);
  }

  /**
   * Execute a allocate ids statement.
   *
   * @param statement
   *          the statement to execute.
   * @return the result of the allocate ids request.
   */
  public AllocateIdsResult execute(final AllocateIds statement) throws DatastoreException {
    return Futures.get(executeAsync(statement), DatastoreException.class);
  }

  /**
   * Execute a allocate ids statement.
   *
   * @param statement
   *          the statement to execute.
   * @return the result of the allocate ids request.
   */
  public CompletableFuture<AllocateIdsResult> executeAsync(final AllocateIds statement) {
    final HttpResponse httpResponse;
    try {
      final DatastoreV1.AllocateIdsRequest.Builder request = DatastoreV1.AllocateIdsRequest
          .newBuilder().addAllKey(statement.getPb(config.getNamespace()));
      final ProtoHttpContent payload = new ProtoHttpContent(request.build());
      httpResponse = executeRequest("allocateIds", payload);

      final DatastoreV1.AllocateIdsResponse allocate = DatastoreV1.AllocateIdsResponse
          .parseFrom(streamResponse(httpResponse));
      return completedFuture(AllocateIdsResult.build(allocate));

    } catch (final Exception e) {
      return exceptionally(e);
    }
  }

  /**
   * Execute a keyed query statement.
   *
   * @param statement
   *          the statement to execute.
   * @return the result of the query request.
   */
  public QueryResult execute(final KeyQuery statement) throws DatastoreException {
    return Futures.get(executeAsync(statement), DatastoreException.class);
  }

  /**
   * Execute a keyed query statement.
   *
   * @param statement
   *          the statement to execute.
   * @return the result of the query request.
   */
  public CompletableFuture<QueryResult> executeAsync(final KeyQuery statement) {
    return executeAsync(statement, completedFuture(TransactionResult.build()));
  }

  /**
   * Execute a keyed query statement in a given transaction.
   *
   * @param statement
   *          the statement to execute.
   * @param txn
   *          the transaction to execute the query.
   * @return the result of the query request.
   */
  public QueryResult execute(final KeyQuery statement, final TransactionResult txn)
      throws DatastoreException {
    return Futures.get(executeAsync(statement, CompletableFuture.completedFuture(txn)),
        DatastoreException.class);
  }

  /**
   * Execute a keyed query statement in a given transaction.
   *
   * @param statement
   *          the statement to execute.
   * @param txn
   *          the transaction to execute the query.
   * @return the result of the query request.
   */
  public CompletableFuture<QueryResult> executeAsync(final KeyQuery statement,
      final Future<TransactionResult> txn) {
    try {
      final HttpResponse httpResponse;
      final DatastoreV1.Key key = statement.getKey().getPb(config.getNamespace());
      final DatastoreV1.LookupRequest.Builder request = DatastoreV1.LookupRequest.newBuilder()
          .addKey(key);
      final ByteString transaction = txn.get().getTransaction();
      if (transaction != null) {
        request.setReadOptions(DatastoreV1.ReadOptions.newBuilder().setTransaction(transaction));
      }
      final ProtoHttpContent payload = new ProtoHttpContent(request.build());
      httpResponse = executeRequest("lookup", payload);

      final DatastoreV1.LookupResponse query = DatastoreV1.LookupResponse
          .parseFrom(streamResponse(httpResponse));
      return completedFuture(QueryResult.build(query));

    } catch (Exception e) {
      return exceptionally(e);
    }
  }

  /**
   * Execute a mutation query statement.
   *
   * @param statement
   *          the statement to execute.
   * @return the result of the mutation request.
   */
  public MutationResult execute(final MutationStatement statement) throws DatastoreException {
    return Futures.get(executeAsync(statement), DatastoreException.class);
  }

  /**
   * Execute a mutation query statement.
   *
   * @param statement
   *          the statement to execute.
   * @return the result of the mutation request.
   */
  public CompletableFuture<MutationResult> executeAsync(final MutationStatement statement) {
    return executeAsync(statement, CompletableFuture.completedFuture(TransactionResult.build()));
  }

  /**
   * Execute a mutation query statement in a given transaction.
   *
   * @param statement
   *          the statement to execute.
   * @param txn
   *          the transaction to execute the query.
   * @return the result of the mutation request.
   */
  public MutationResult execute(final MutationStatement statement, final TransactionResult txn)
      throws DatastoreException {
    return Futures.get(executeAsync(statement, CompletableFuture.completedFuture(txn)),
        DatastoreException.class);
  }

  /**
   * Execute a mutation query statement in a given transaction.
   *
   * @param statement
   *          the statement to execute.
   * @param txn
   *          the transaction to execute the query.
   * @return the result of the mutation request.
   */
  public CompletableFuture<MutationResult> executeAsync(final MutationStatement statement,
      final Future<TransactionResult> txn) {
    try {

      final HttpResponse httpResponse;
      final DatastoreV1.CommitRequest.Builder request = DatastoreV1.CommitRequest.newBuilder();
      if (statement != null) {
        request.setMutation(statement.getPb(config.getNamespace()));
      }
      final ByteString transaction = txn.get().getTransaction();
      if (transaction != null) {
        request.setTransaction(transaction);
      } else {
        request.setMode(DatastoreV1.CommitRequest.Mode.NON_TRANSACTIONAL);
      }
      final ProtoHttpContent payload = new ProtoHttpContent(request.build());
      httpResponse = executeRequest("commit", payload);

      final DatastoreV1.CommitResponse commit = DatastoreV1.CommitResponse
          .parseFrom(streamResponse(httpResponse));
      return completedFuture(MutationResult.build(commit));
    } catch (Exception e) {
      return exceptionally(e);
    }
  }

  /**
   * Execute a query statement.
   *
   * @param statement
   *          the statement to execute.
   * @return the result of the query request.
   */
  public QueryResult execute(final Query statement) throws DatastoreException {
    return Futures.get(executeAsync(statement), DatastoreException.class);
  }

  /**
   * Execute a query statement.
   *
   * @param statement
   *          the statement to execute.
   * @return the result of the query request.
   */
  public Future<QueryResult> executeAsync(final Query statement) {
    return executeAsync(statement, completedFuture(TransactionResult.build()));
  }

  /**
   * Execute a query statement in a given transaction.
   *
   * @param statement
   *          the statement to execute.
   * @param txn
   *          the transaction to execute the query.
   * @return the result of the query request.
   */
  public QueryResult execute(final Query statement, final TransactionResult txn)
      throws DatastoreException {
    return Futures.get(executeAsync(statement, CompletableFuture.completedFuture(txn)),
        DatastoreException.class);
  }

  /**
   * Execute a query statement in a given transaction.
   *
   * @param statement
   *          the statement to execute.
   * @param txn
   *          the transaction to execute the query.
   * @return the result of the query request.
   */
  public Future<QueryResult> executeAsync(final Query statement, final Future<TransactionResult> txn) {
    try {

      final HttpResponse httpResponse;
      final DatastoreV1.RunQueryRequest.Builder request = DatastoreV1.RunQueryRequest.newBuilder()
          .setQuery(statement.getPb());
      final String namespace = config.getNamespace();
      if (namespace != null) {
        request.setPartitionId(DatastoreV1.PartitionId.newBuilder().setNamespace(namespace));
      }
      final ByteString transaction = txn.get().getTransaction();
      if (transaction != null) {
        request.setReadOptions(DatastoreV1.ReadOptions.newBuilder().setTransaction(transaction));
      }
      final ProtoHttpContent payload = new ProtoHttpContent(request.build());
      httpResponse = executeRequest("runQuery", payload);

      final DatastoreV1.RunQueryResponse query = DatastoreV1.RunQueryResponse
          .parseFrom(streamResponse(httpResponse));
      return completedFuture(QueryResult.build(query));

    } catch (Exception e) {
      return exceptionally(e);
    }
  }

  static <T> CompletableFuture<T> exceptionally(Exception e) {
    CompletableFuture<T> result = new CompletableFuture<>();
    result.completeExceptionally(new DatastoreException(e));
    return result;
  }

  static void exceptionally(String message) throws DatastoreException {
    throw new DatastoreException(message);
  }

  static void exceptionally(int code, String message) throws DatastoreException {
    throw new DatastoreException(code, message);
  }

  static void exceptionally(HttpResponse httpResponse) throws DatastoreException {
    int statusCode = httpResponse.getStatusLine().getStatusCode();
    try {
      exceptionally(statusCode, EntityUtils.toString(httpResponse.getEntity(), "UTF-8"));
    } catch (IOException e) {
      throw new DatastoreException(statusCode, "Error parsing HTTP response:" + e.getMessage());
    }
  }
}
