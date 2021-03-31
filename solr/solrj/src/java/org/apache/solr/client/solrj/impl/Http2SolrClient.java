/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.client.solrj.impl;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpStatus;
import org.apache.http.entity.ContentType;
import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.V2RequestSupport;
import org.apache.solr.client.solrj.embedded.SSLConfig;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.util.AsyncListener;
import org.apache.solr.client.solrj.util.Cancellable;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.StringUtils;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.QoSParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.Base64;
import org.apache.solr.common.util.CloseTracker;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.SolrInternalHttpClient;
import org.apache.solr.common.util.SolrQueuedThreadPool;
import org.apache.solr.common.util.Utils;
import org.eclipse.jetty.client.ConnectionPool;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.HttpDestination;
import org.eclipse.jetty.client.MultiplexConnectionPool;
import org.eclipse.jetty.client.ProtocolHandlers;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.client.http.HttpClientTransportOverHTTP;
import org.eclipse.jetty.client.util.ByteBufferContentProvider;
import org.eclipse.jetty.client.util.BytesContentProvider;
import org.eclipse.jetty.client.util.FormContentProvider;
import org.eclipse.jetty.client.util.InputStreamContentProvider;
import org.eclipse.jetty.client.util.InputStreamResponseListener;
import org.eclipse.jetty.client.util.MultiPartContentProvider;
import org.eclipse.jetty.client.util.OutputStreamContentProvider;
import org.eclipse.jetty.client.util.StringContentProvider;
import org.eclipse.jetty.http.HttpField;
import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.http2.client.HTTP2Client;
import org.eclipse.jetty.http2.client.http.HttpClientTransportOverHTTP2;
import org.eclipse.jetty.util.BlockingArrayQueue;
import org.eclipse.jetty.util.Fields;
import org.eclipse.jetty.util.Pool;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.ScheduledExecutorScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.client.solrj.impl.BaseHttpSolrClient.RemoteExecutionException;
import static org.apache.solr.client.solrj.impl.BaseHttpSolrClient.RemoteSolrException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.lang.management.ManagementFactory;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

/**
 * Difference between this {@link Http2SolrClient} and {@link HttpSolrClient}:
 * <ul>
 *  <li>{@link Http2SolrClient} sends requests in HTTP/2</li>
 *  <li>{@link Http2SolrClient} can point to multiple urls</li>
 *  <li>{@link Http2SolrClient} does not expose its internal httpClient like {@link HttpSolrClient#getHttpClient()},
 * sharing connection pools should be done by {@link Http2SolrClient.Builder#withHttpClient(Http2SolrClient)} </li>
 * </ul>
 */
public class Http2SolrClient extends SolrClient {

  public static final int PROC_COUNT = ManagementFactory.getOperatingSystemMXBean().getAvailableProcessors();

  public static final String REQ_PRINCIPAL_KEY = "solr-req-principal";

  private static volatile SSLConfig defaultSSLConfig;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String POST = "POST";
  private static final String PUT = "PUT";
  private static final String GET = "GET";
  private static final String DELETE = "DELETE";
  private static final String HEAD = "HEAD";

  private static final String AGENT = "Solr[" + Http2SolrClient.class.getName() + "] 2.0";
  private static final Charset FALLBACK_CHARSET = StandardCharsets.UTF_8;
  private static final String DEFAULT_PATH = "/select";

  private final Map<String, String> headers;
  private final boolean markedInternal;

  private CloseTracker closeTracker;

  private final HttpClient httpClient;
  private volatile Set<String> queryParams = Collections.emptySet();
  private final int idleTimeout;
  private final boolean strictEventOrdering;
  private volatile ResponseParser parser = new BinaryResponseParser();
  private volatile RequestWriter requestWriter = new BinaryRequestWriter();
  private final Set<HttpListenerFactory> listenerFactory = ConcurrentHashMap.newKeySet();
  private final AsyncTracker asyncTracker;
  /**
   * The URL of the Solr server.
   */
  private volatile String serverBaseUrl;
  private volatile boolean closeClient;
  private volatile boolean closed;

  protected Http2SolrClient(String serverBaseUrl, Builder builder) {
    assert (closeTracker = new CloseTracker()) != null;
    assert builder.http2SolrClient != null || ObjectReleaseTracker.track(this);
    if (serverBaseUrl != null)  {
      if (!serverBaseUrl.equals("/") && !serverBaseUrl.isEmpty() && serverBaseUrl.charAt(serverBaseUrl.length() - 1) == '/') {
        serverBaseUrl = serverBaseUrl.substring(0, serverBaseUrl.length() - 1);
      }

      if (serverBaseUrl.startsWith("//")) {
        serverBaseUrl = serverBaseUrl.substring(1);
      }
      this.serverBaseUrl = serverBaseUrl;
    }
    int maxOutstandingAsyncRequests = 1024;
    if (builder.maxOutstandingAsyncRequests != null) maxOutstandingAsyncRequests = builder.maxOutstandingAsyncRequests;
    asyncTracker = new AsyncTracker(maxOutstandingAsyncRequests);
    this.headers = builder.headers;
    this.markedInternal = builder.markedInternal;

    this.strictEventOrdering = builder.strictEventOrdering;

    if (builder.idleTimeout != null && builder.idleTimeout > 0) idleTimeout = builder.idleTimeout;
    else idleTimeout = HttpClientUtil.DEFAULT_SO_TIMEOUT;

    if (builder.http2SolrClient == null) {
      httpClient = createHttpClient(builder);
      closeClient = true;
    } else {
      httpClient = builder.http2SolrClient.httpClient;
    }
  }

  public void addListenerFactory(HttpListenerFactory factory) {
    this.listenerFactory.add(factory);
  }

  // internal usage only
  public HttpClient getHttpClient() {
    return httpClient;
  }

  public void addHeaders(Map<String,String> headers) {
    this.headers.putAll(headers);
  }

  // internal usage only
  ProtocolHandlers getProtocolHandlers() {
    return httpClient.getProtocolHandlers();
  }

  private HttpClient createHttpClient(Builder builder) {
    HttpClient httpClient;

    SslContextFactory.Client sslContextFactory = null;

    if (builder.sslConfig == null) {
      sslContextFactory = getDefaultSslContextFactory();
    } else {
      sslContextFactory = builder.sslConfig.createClientContextFactory();
    }
    // MRM TODO: - look at config again as well
    int minThreads = Integer.getInteger("solr.minHttp2ClientThreads", PROC_COUNT << 1);

    minThreads = Math.min( builder.maxThreadPoolSize, minThreads);

    int maxThreads = Math.max(builder.maxThreadPoolSize, minThreads);

    int capacity = minThreads << 6;
    BlockingQueue<Runnable> queue = new BlockingArrayQueue<>(capacity, 16);

    SolrQueuedThreadPool httpClientExecutor = new SolrQueuedThreadPool("http2Client", maxThreads, minThreads, 600000, queue, -1, null);
    httpClientExecutor.setLowThreadsThreshold(-1);
    try {
      httpClientExecutor.start();
    } catch (Exception e) {
      e.printStackTrace();
    }

    ScheduledExecutorScheduler scheduler = new ScheduledExecutorScheduler("http2client-scheduler", true);

    if (builder.useHttp1_1) {
      if (log.isTraceEnabled()) log.trace("Create Http2SolrClient with HTTP/1.1 transport");

      SolrHttpClientTransportOverHTTP transport = new SolrHttpClientTransportOverHTTP(2);
      httpClient = new SolrInternalHttpClient(transport, sslContextFactory, httpClientExecutor, scheduler);
    } else {
      if (log.isTraceEnabled()) log.trace("Create Http2SolrClient with HTTP/2 transport");
//
//          if (log.isDebugEnabled()) {
//            RuntimeException e = new RuntimeException();
//            StackTraceElement[] stack = e.getStackTrace();
//            for (int i = 0; i < Math.min(8, stack.length - 1); i++) {
//              log.debug(stack[i].toString());
//            }
//
//            log.debug("create http2solrclient {}", this);
//          }

      HTTP2Client http2client = new HTTP2Client();
      http2client.setSelectors(2);
      http2client.setMaxConcurrentPushedStreams(1024);
      http2client.setInputBufferSize(8192);

      HttpClientTransportOverHTTP2 transport = new HttpClientTransportOverHTTP2(http2client);
      transport.setConnectionPoolFactory(new MyFactory());
      httpClient = new SolrInternalHttpClient(transport, sslContextFactory, httpClientExecutor, scheduler);
    }

    try {
      httpClient.setScheduler(scheduler);
      httpClient.manage(scheduler);
      httpClient.setExecutor(httpClientExecutor);
      httpClient.manage(httpClientExecutor);
      httpClient.setStrictEventOrdering(strictEventOrdering);
      // httpClient.setSocketAddressResolver(new SocketAddressResolver.Sync());
      httpClient.setConnectBlocking(false);
      httpClient.setFollowRedirects(false);
      if (builder.maxConnectionsPerHost != null) httpClient.setMaxConnectionsPerDestination(builder.maxConnectionsPerHost);
      httpClient.setMaxRequestsQueuedPerDestination(builder.maxRequestsQueuedPerDestination);
      httpClient.setRequestBufferSize(8192);
      httpClient.setUserAgentField(new HttpField(HttpHeader.USER_AGENT, AGENT));
      httpClient.setIdleTimeout(builder.idleTimeout);
      httpClient.setTCPNoDelay(true);
      httpClient.setStopTimeout(5000);
      httpClient.setAddressResolutionTimeout(3000);
      if (builder.connectionTimeout != null) httpClient.setConnectTimeout(builder.connectionTimeout);
      httpClient.start();
    } catch (Exception e) {
      log.error("Exception creating HttpClient", e);
      try {
        close();
      } catch (Exception e1) {
        e.addSuppressed(e1);
      }
      throw new RuntimeException(e);
    }
    return httpClient;
  }

  public void close() {
    if (log.isTraceEnabled()) log.trace("Closing {} closeClient={}", this.getClass().getSimpleName(), closeClient);
    assert closeTracker != null ? closeTracker.close() : true;
    closed = true;
    try {
      org.apache.solr.common.util.IOUtils.closeQuietly(asyncTracker);

      if (closeClient) {
        try {
          httpClient.stop();
        } catch (Exception e) {
          log.error("Exception closing httpClient", e);
        }
      }
      if (log.isTraceEnabled()) log.trace("Done closing {}", this.getClass().getSimpleName());
    } finally {
      assert ObjectReleaseTracker.release(this);
    }
  }

  public void waitForOutstandingRequests(long timeout, TimeUnit timeUnit) {
    asyncTracker.waitForComplete(timeout, timeUnit);
  }

  public boolean isV2ApiRequest(final SolrRequest request) {
    return request instanceof V2Request || request.getPath().contains("/____v2");
  }

  public long getIdleTimeout() {
    return idleTimeout;
  }

  public static class OutStream implements Closeable{
    private final String origCollection;
    private final ModifiableSolrParams origParams;
    private final OutputStreamContentProvider outProvider;
    private final InputStreamResponseListener responseListener;
    private final boolean isXml;

    public OutStream(String origCollection, ModifiableSolrParams origParams,
                     OutputStreamContentProvider outProvider, InputStreamResponseListener responseListener, boolean isXml) {
      this.origCollection = origCollection;
      this.origParams = origParams;
      this.outProvider = outProvider;
      this.responseListener = responseListener;
      this.isXml = isXml;
    }

    boolean belongToThisStream(@SuppressWarnings({"rawtypes"})SolrRequest solrRequest, String collection) {
      ModifiableSolrParams solrParams = new ModifiableSolrParams(solrRequest.getParams());
      return origParams.toNamedList().equals(solrParams.toNamedList()) && StringUtils.equals(origCollection, collection);
    }

    public void write(byte[] b) throws IOException {
      this.outProvider.getOutputStream().write(b);
    }

    public void flush() throws IOException {
      this.outProvider.getOutputStream().flush();
    }

    @Override
    public void close() throws IOException {
      if (isXml) {
        write("</stream>".getBytes(FALLBACK_CHARSET));
      }
      this.outProvider.getOutputStream().close();
    }

    //TODO this class should be hidden
    public InputStreamResponseListener getResponseListener() {
      return responseListener;
    }
  }

  public OutStream initOutStream(String baseUrl,
                                 UpdateRequest updateRequest,
                                 String collection) throws IOException {
    String contentType = requestWriter.getUpdateContentType();
    final ModifiableSolrParams origParams = new ModifiableSolrParams(updateRequest.getParams());

    // The parser 'wt=' and 'version=' params are used instead of the
    // original params
    ModifiableSolrParams requestParams = new ModifiableSolrParams(origParams);
    requestParams.set(CommonParams.WT, parser.getWriterType());
    requestParams.set(CommonParams.VERSION, parser.getVersion());

    String basePath = baseUrl;
    if (collection != null)
      basePath += "/" + collection;
    if (!(!basePath.isEmpty() && basePath.charAt(basePath.length() - 1) == '/'))
      basePath += "/";

    OutputStreamContentProvider provider = new OutputStreamContentProvider();
    Request postRequest = httpClient
        .newRequest(basePath + "update"
            + requestParams.toQueryString())
        .method(HttpMethod.POST)
        .header(HttpHeader.CONTENT_TYPE, contentType)
        .content(provider);
    postRequest.idleTimeout(httpClient.getIdleTimeout(), TimeUnit.MILLISECONDS);
    headers.forEach(postRequest::header);

    decorateRequest(postRequest, updateRequest);
    updateRequest.setBasePath(baseUrl);
    InputStreamResponseListener responseListener = new InputStreamResponseListener();
    postRequest.send(responseListener);

    boolean isXml = ClientUtils.TEXT_XML.equals(requestWriter.getUpdateContentType());
    OutStream outStream = new OutStream(collection, origParams, provider, responseListener,
        isXml);
    if (isXml) {
      outStream.write("<stream>".getBytes(FALLBACK_CHARSET));
    }
    return outStream;
  }

  public void send(OutStream outStream, SolrRequest req, String collection) throws IOException {
    assert outStream.belongToThisStream(req, collection);
    this.requestWriter.write(req, outStream.outProvider.getOutputStream());
    if (outStream.isXml) {
      // check for commit or optimize
      SolrParams params = req.getParams();
      if (params != null) {
        String fmt = null;
        if (params.getBool(UpdateParams.OPTIMIZE, false)) {
          fmt = "<optimize waitSearcher=\"%s\" />";
        } else if (params.getBool(UpdateParams.COMMIT, false)) {
          fmt = "<commit waitSearcher=\"%s\" />";
        }
        if (fmt != null) {
          byte[] content = String.format(Locale.ROOT,
              fmt, params.getBool(UpdateParams.WAIT_SEARCHER, false)
                  + "")
              .getBytes(FALLBACK_CHARSET);
          outStream.write(content);
        }
      }
    }
  }

  private static final Exception CANCELLED_EXCEPTION = new Exception();
  private static final Cancellable FAILED_MAKING_REQUEST_CANCELLABLE = () -> {};

  public Cancellable asyncRequest(@SuppressWarnings({"rawtypes"}) SolrRequest solrRequest, String collection, AsyncListener<NamedList<Object>> asyncListener) {
    Integer idleTimeout = solrRequest.getParams().getInt("idleTimeout");
    Request req;
    try {
      req = makeRequest(solrRequest, collection);
      if (idleTimeout != null) {
        req.idleTimeout(idleTimeout, TimeUnit.MILLISECONDS);
      }
    } catch (Exception e) {
      asyncListener.onFailure(e, 500);
      return FAILED_MAKING_REQUEST_CANCELLABLE;
    }
    final ResponseParser parser = solrRequest.getResponseParser() == null
        ? this.parser: solrRequest.getResponseParser();
    asyncTracker.register();
    try {

      InputStreamResponseListener isl = new InputStreamResponseListener() {

        @Override
        public void onHeaders(Response response) {
          super.onHeaders(response);
          if (log.isTraceEnabled()) log.trace("async response ready");
          httpClient.getExecutor().execute(() -> {
            try {
              NamedList<Object> body = processErrorsAndResponse(solrRequest, parser, getInputStream(), response);

              asyncListener.onSuccess(body);
            } catch (Exception e) {
              if (SolrException.getRootCause(e) != CANCELLED_EXCEPTION) {
                asyncListener.onFailure(e, e instanceof SolrException ? ((SolrException) e).code() : 500);
              }
            }

          });
        }

        @Override
        public void onSuccess(Response response) {

          try {
            super.onSuccess(response);
          } finally {
            asyncTracker.arrive();
          }

        }

        @Override
        public void onFailure(Response response, Throwable failure) {
          try {
            super.onFailure(response, failure);

            if (SolrException.getRootCause(failure) != CANCELLED_EXCEPTION) {
              asyncListener.onFailure(failure, response.getStatus());
            } else {
              asyncListener.onSuccess(new NamedList<>());
            }

          } finally {
            asyncTracker.arrive();
          }
        }
      };

      req.send(isl);

    } catch (Exception e) {
      log.debug("failed sending request", e);
      if (e != CANCELLED_EXCEPTION) {
        asyncListener.onFailure(e, 500);
      }
      //log.info("UNREGISTER TRACKER");
     // asyncTracker.arrive();
    }

    return new AbortRequest(req);
  }

  public Cancellable asyncRequestRaw(@SuppressWarnings({"rawtypes"}) SolrRequest solrRequest, String collection, AsyncListener<InputStream> asyncListener) {
    Request req;
    try {
      req = makeRequest(solrRequest, collection);
    } catch (Exception e) {
      asyncListener.onFailure(e, 500);
      return FAILED_MAKING_REQUEST_CANCELLABLE;
    }
    MyInputStreamResponseListener mysl = new MyInputStreamResponseListener(httpClient, asyncListener);
    try {
      req.send(mysl);
    } catch (Exception e) {
      asyncListener.onFailure(e, 500);

      throw new SolrException(SolrException.ErrorCode.UNKNOWN, e);
    }

    return new MyCancellable(req, mysl);
  }

  @Override
  public NamedList<Object> request(@SuppressWarnings({"rawtypes"}) SolrRequest solrRequest, String collection) throws SolrServerException, IOException {
    Request req = makeRequest(solrRequest, collection);

    return request(solrRequest, req);
  }


  public NamedList<Object> request(@SuppressWarnings({"rawtypes"}) SolrRequest solrRequest, Request req) throws SolrServerException, IOException {

    final ResponseParser parser = solrRequest.getResponseParser() == null ? this.parser : solrRequest.getResponseParser();


    if (wantStream(parser)) {
      InputStreamResponseListener listener = new InputStreamResponseListener();


      // no processor specified, return raw stream
      NamedList<Object> rsp = new NamedList<>(1);
      rsp.add("stream", listener.getInputStream());

      return rsp;
    }

    ContentResponse response;
    try {
     response = req.send();
    } catch (Exception e) {
      throw new SolrServerException(e);
    }

    return processErrorsAndResponse(solrRequest, parser, new ByteArrayInputStream(response.getContent()), response);
  }

  private ContentType getContentType(Response response) {
    String contentType = response.getHeaders().get(HttpHeader.CONTENT_TYPE);
    return StringUtils.isEmpty(contentType)? null : ContentType.parse(contentType);
  }

  private void setBasicAuthHeader(SolrRequest solrRequest, Request req) {
    if (solrRequest.getBasicAuthUser() != null && solrRequest.getBasicAuthPassword() != null) {
      String userPass = solrRequest.getBasicAuthUser() + ":" + solrRequest.getBasicAuthPassword();
      String encoded = Base64.byteArrayToBase64(userPass.getBytes(FALLBACK_CHARSET));
      req.header("Authorization", "Basic " + encoded);
    }
  }

  public void setBaseUrl(String baseUrl) {
    this.serverBaseUrl = baseUrl;
  }

  public Request makeRequest(SolrRequest solrRequest, String collection)
      throws SolrServerException, IOException {
    Request req = createRequest(solrRequest, collection);
    decorateRequest(req, solrRequest);
    return req;
  }

  private void decorateRequest(Request req, SolrRequest solrRequest) {
    req.header(HttpHeader.ACCEPT_ENCODING, null).idleTimeout(httpClient.getIdleTimeout(), TimeUnit.MILLISECONDS);
    if (solrRequest.getUserPrincipal() != null) {
      req.attribute(REQ_PRINCIPAL_KEY, solrRequest.getUserPrincipal());
    }

    setBasicAuthHeader(solrRequest, req);
    for (HttpListenerFactory factory : listenerFactory) {
      HttpListenerFactory.RequestResponseListener listener = factory.get();
      listener.onQueued(req);
      req.onRequestBegin(listener);
      req.onComplete(listener);
    }

    Map<String, String> headers = solrRequest.getHeaders();
    if (headers != null) {
      headers.forEach(req::header);
    }
  }
  
  private String changeV2RequestEndpoint(String basePath) throws MalformedURLException {
    URL oldURL = new URL(basePath);
    String newPath = oldURL.getPath().replaceFirst("/solr", "/api");
    return new URL(oldURL.getProtocol(), oldURL.getHost(), oldURL.getPort(), newPath).toString();
  }

  public boolean isMarkedInternal() {
    return markedInternal;
  }

  private Request createRequest(SolrRequest solrRequest, String collection) throws IOException, SolrServerException {
    if (solrRequest.getBasePath() == null && serverBaseUrl == null)
      throw new IllegalArgumentException("Destination node is not provided!");

    if (solrRequest instanceof V2RequestSupport) {
      solrRequest = ((V2RequestSupport) solrRequest).getV2Request();
    }
    SolrParams params = solrRequest.getParams();
    RequestWriter.ContentWriter contentWriter = requestWriter.getContentWriter(solrRequest);
    Collection<ContentStream> streams = contentWriter == null ? requestWriter.getContentStreams(solrRequest) : null;
    String path = requestWriter.getPath(solrRequest);
    if (path == null) {
      path = DEFAULT_PATH;
    }

    ResponseParser parser = solrRequest.getResponseParser();
    if (parser == null) {
      parser = this.parser;
    }

    // The parser 'wt=' and 'version=' params are used instead of the original
    // params
    ModifiableSolrParams wparams = new ModifiableSolrParams(params);
    if (parser != null) {
      wparams.set(CommonParams.WT, parser.getWriterType());
      wparams.set(CommonParams.VERSION, parser.getVersion());
    }

    //TODO add invariantParams support

    String basePath = solrRequest.getBasePath() == null ? serverBaseUrl : solrRequest.getBasePath();
    if (collection != null)
      basePath += "/" + collection;

    if (solrRequest instanceof V2Request) {
      if (System.getProperty("solr.v2RealPath") == null) {
        basePath = changeV2RequestEndpoint(basePath);
      } else {
        basePath = solrRequest.getBasePath() == null ? serverBaseUrl  : solrRequest.getBasePath() + "/____v2";
      }
    }

    if (SolrRequest.METHOD.GET == solrRequest.getMethod()) {
      if (streams != null || contentWriter != null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "GET can't send streams!");
      }

      Request req = httpClient.newRequest(basePath + path + wparams.toQueryString()).method(HttpMethod.GET);
      for (Map.Entry<String,String> entry : headers.entrySet()) {
        req = req.header(entry.getKey(), entry.getValue());
      }
      return req;
    }

    if (SolrRequest.METHOD.DELETE == solrRequest.getMethod()) {
      Request req = httpClient.newRequest(basePath + path + wparams.toQueryString()).method(HttpMethod.DELETE);
      for (Map.Entry<String,String> entry : headers.entrySet()) {
        req = req.header(entry.getKey(), entry.getValue());
      }
      return req;
    }

    if (SolrRequest.METHOD.POST == solrRequest.getMethod() || SolrRequest.METHOD.PUT == solrRequest.getMethod()) {

      String url = basePath + path;
      boolean hasNullStreamName = false;
      if (streams != null) {
        hasNullStreamName = streams.stream().anyMatch(new ContentStreamPredicate());
      }

      boolean isMultipart = streams != null && streams.size() > 1 && !hasNullStreamName;

      HttpMethod method = SolrRequest.METHOD.POST == solrRequest.getMethod() ? HttpMethod.POST : HttpMethod.PUT;

      if (contentWriter != null) {
        Request req;
        try {
          req = httpClient.newRequest(url + wparams.toQueryString()).idleTimeout(httpClient.getIdleTimeout(), TimeUnit.MILLISECONDS).method(method);
        } catch (IllegalArgumentException e) {
          throw new SolrServerException("Illegal url for request url=" + url, e);
        }
        for (Map.Entry<String,String> entry : headers.entrySet()) {
          req = req.header(entry.getKey(), entry.getValue());
        }
        //req = req.idleTimeout(httpClient.getIdleTimeout(), TimeUnit.MILLISECONDS);


        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        contentWriter.write(baos);

        BytesContentProvider bcp = new BytesContentProvider(contentWriter.getContentType(), baos.toByteArray());
        Request r = req.content(bcp, contentWriter.getContentType());

        return r;
      } else if (streams == null || isMultipart) {
        // send server list and request list as query string params
        ModifiableSolrParams queryParams = calculateQueryParams(this.queryParams, wparams);
        queryParams.add(calculateQueryParams(solrRequest.getQueryParams(), wparams));
        Request req = httpClient
            .newRequest(url + queryParams.toQueryString())
            .idleTimeout(httpClient.getIdleTimeout(), TimeUnit.MILLISECONDS)
            .method(method);
        for (Map.Entry<String,String> entry : headers.entrySet()) {
          req = req.header(entry.getKey(), entry.getValue());
        }
        return fillContentStream(req, streams, wparams, isMultipart);
      } else {
        // It is has one stream, it is the post body, put the params in the URL
        ContentStream contentStream = streams.iterator().next();
        Request req = httpClient
                .newRequest(url + wparams.toQueryString())
                .method(method)
                .idleTimeout(httpClient.getIdleTimeout(), TimeUnit.MILLISECONDS)
                .content(new InputStreamContentProvider(contentStream.getStream()), contentStream.getContentType());
        for (Map.Entry<String,String> entry : headers.entrySet()) {
          req = req.header(entry.getKey(), entry.getValue());
        }
        return req;
      }
    }

    throw new SolrServerException("Unsupported method: " + solrRequest.getMethod());
  }

  private Request fillContentStream(Request req, Collection<ContentStream> streams,
                                    ModifiableSolrParams wparams,
                                    boolean isMultipart) throws IOException {
    if (isMultipart) {
      // multipart/form-data
      MultiPartContentProvider content = new MultiPartContentProvider();
      Iterator<String> iter = wparams.getParameterNamesIterator();
      while (iter.hasNext()) {
        String key = iter.next();
        String[] vals = wparams.getParams(key);
        if (vals != null) {
          for (String val : vals) {
            content.addFieldPart(key, new StringContentProvider(val), null);
          }
        }
      }
      if (streams != null) {
        for (ContentStream contentStream : streams) {
          String contentType = contentStream.getContentType();
          if (contentType == null) {
            contentType = BinaryResponseParser.BINARY_CONTENT_TYPE; // default
          }
          String name = contentStream.getName();
          if (name == null) {
            name = "";
          }
          HttpFields fields = new HttpFields();
          fields.add(HttpHeader.CONTENT_TYPE, contentType);
          content.addFilePart(name, contentStream.getName(), new InputStreamContentProvider(contentStream.getStream()), fields);
        }
      }
      content.close();
      req = req.content(content);
    } else {
      // application/x-www-form-urlencoded
      Fields fields = new Fields();
      Iterator<String> iter = wparams.getParameterNamesIterator();
      while (iter.hasNext()) {
        String key = iter.next();
        String[] vals = wparams.getParams(key);
        if (vals != null) {
          for (String val : vals) {
            fields.add(key, val);
          }
        }
      }
      req.content(new FormContentProvider(fields, FALLBACK_CHARSET));
    }

    return req;
  }

  private boolean wantStream(final ResponseParser processor) {
    return processor == null || processor instanceof InputStreamResponseParser;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private NamedList<Object> processErrorsAndResponse(SolrRequest solrRequest, final ResponseParser processor,
      InputStream is, Response response) {

    boolean isV2Api = isV2ApiRequest(solrRequest);
    boolean shouldClose = true;

    try {

//      try {
//        response = listener.get(idleTimeout, TimeUnit.MILLISECONDS);
//      } catch (Exception e) {
//        throw new SolrException(SolrException.ErrorCode.UNKNOWN, e);
//      }

      ContentType contentType = getContentType(response);
      String mimeType = null;
      String encoding = null;
      if (contentType != null) {
        mimeType = contentType.getMimeType();
        encoding = contentType.getCharset() != null? contentType.getCharset().name() : null;
      }

      String procCt = processor.getContentType();
      if (procCt != null && mimeType != null) {
        String procMimeType = ContentType.parse(procCt).getMimeType().trim()
            .toLowerCase(Locale.ROOT);

        if (!procMimeType.equals(mimeType)) {
          // unexpected mime type
          String msg =
              "Expected mime type " + procMimeType + " but got " + mimeType
                  + ".";
          String exceptionEncoding =
              encoding != null ? encoding : FALLBACK_CHARSET.name();
          try {
            msg = msg + " " + IOUtils.toString(is, exceptionEncoding);
          } catch (Exception e) {
            try {
              throw new RemoteSolrException(serverBaseUrl, response.getStatus(),
                  "Could not parse response with encoding " + exceptionEncoding,
                  e);
            } catch (Exception e1) {
              log.warn("", e1);
            }
          }
          throw new RemoteSolrException(serverBaseUrl, -1, msg, null);
        }
      }

      NamedList<Object> rsp;
      int httpStatus = -1;

      try {
        httpStatus = response.getStatus();
      } catch (Exception e) {
        log.warn("", e);
      }

      if (httpStatus == 404) {
        throw new RemoteSolrException(response.getRequest().getURI().toString(), httpStatus, "not found: " + httpStatus
            + ", message:" + response.getReason(),
            null);
      }

      try {
        rsp = processor.processResponse(is, encoding);
      } catch (Exception e) {
        try {

          if (httpStatus == 200) {
            return new NamedList<>();
          }
          throw new RemoteSolrException(serverBaseUrl, httpStatus, "status: " + httpStatus, e);
        } catch (Exception e1) {
          log.warn("", e1);
        }

        throw new RemoteSolrException(serverBaseUrl, 527, "", e);
      }

      // log.error("rsp:{}", rsp);

      Object error = rsp == null ? null : rsp.get("error");

      if (error != null && (error instanceof NamedList && ((NamedList<?>) error).get("metadata") == null || isV2Api)) {
        throw RemoteExecutionException.create(serverBaseUrl, rsp);
      }

      if (httpStatus != HttpStatus.SC_OK && !isV2Api) {
        NamedList<String> metadata = null;
        String reason = null;
        try {
          if (error != null) {
            reason = (String) Utils.getObjectByPath(error, false, Collections.singletonList("msg"));
            if(reason == null) {
              reason = (String) Utils.getObjectByPath(error, false, Collections.singletonList("trace"));
            }
            Object metadataObj = Utils.getObjectByPath(error, false, Collections.singletonList("metadata"));
            if  (metadataObj instanceof NamedList) {
              metadata = (NamedList<String>) metadataObj;
            } else if (metadataObj instanceof List) {
              // NamedList parsed as List convert to NamedList again
              List<Object> list = (List<Object>) metadataObj;
              final int size = list.size();
              metadata = new NamedList<>(size /2);
              for (int i = 0; i < size; i+=2) {
                metadata.add((String)list.get(i), (String) list.get(i+1));
              }
            } else if (metadataObj instanceof Map) {
              metadata = new NamedList((Map) metadataObj);
            }
            List details = (ArrayList) Utils.getObjectByPath(error, false, Collections.singletonList("details"));
            if (details != null) {
              reason = reason + " " + details;
            }

          }
        } catch (Exception ex) {
          log.warn("Exception parsing error response", ex);
        }
        if (reason == null) {
          String msg = response.getReason() + "\n\n" + "request: " + response.getRequest().getMethod();
          reason = java.net.URLDecoder.decode(msg, FALLBACK_CHARSET);
        }
        log.error("rsp {}", rsp);
        RemoteSolrException rss = new RemoteSolrException(serverBaseUrl, httpStatus, reason, null);
        if (metadata != null) rss.setMetadata(metadata);
        throw rss;
      }

      return rsp;
    } finally {
      try {
        while(true) {
          final int read = is.read();
          if (read == -1) break;
        }
        // is.close();
      } catch (IOException e) {
        // quietly
        log.debug("IOException ensure stream is fully read", e);
      }
    }
  }

  public void enableCloseLock() {
    if (closeTracker != null) {
      closeTracker.enableCloseLock();
    }
  }

  public void disableCloseLock() {
    if (closeTracker != null) {
      closeTracker.disableCloseLock();
    }
  }

  public void setRequestWriter(RequestWriter requestWriter) {
    this.requestWriter = requestWriter;
  }

  public void setFollowRedirects(boolean follow) {
    httpClient.setFollowRedirects(follow);
  }

  public String getBaseURL() {
    return serverBaseUrl;
  }

  private static class MyCancellable implements Cancellable {
    private final Request req;
    private final MyInputStreamResponseListener mysl;

    public MyCancellable(Request req, MyInputStreamResponseListener mysl) {
      this.req = req;
      this.mysl = mysl;
    }

    @Override
    public void cancel() {
      boolean success = req.abort(CANCELLED_EXCEPTION);
    }

    @Override
    public InputStream getStream() {
      return mysl.getInputStream();
    }
  }

  private static class AbortRequest implements Cancellable {
    private final Request req;

    public AbortRequest(Request req) {
      this.req = req;
    }

    @Override
    public void cancel() {
      boolean success = req.abort(CANCELLED_EXCEPTION);
    }
  }

  private static class ContentStreamPredicate implements Predicate<ContentStream> {
    @Override
    public boolean test(ContentStream cs) {
      return cs.getName() == null;
    }
  }

  public abstract static class Abortable {
    public abstract void abort();
  }

  public static class Builder {

    public static int DEFAULT_MAX_THREADS = Integer.getInteger("solr.maxHttp2ClientThreads", 32);
    private static final Integer DEFAULT_IDLE_TIME = Integer.getInteger("solr.http2solrclient.default.idletimeout", 120000);
    public int maxThreadPoolSize = DEFAULT_MAX_THREADS;
    public int maxRequestsQueuedPerDestination = 1024;
    private Http2SolrClient http2SolrClient;
    private SSLConfig sslConfig = defaultSSLConfig;
    private Integer idleTimeout = DEFAULT_IDLE_TIME;
    private Integer connectionTimeout;
    private Integer maxConnectionsPerHost = 32;
    private boolean useHttp1_1 = Boolean.getBoolean("solr.http1");
    protected String baseSolrUrl;
    protected Map<String,String> headers = new HashMap<>(12);
    protected boolean strictEventOrdering = false;
    private Integer maxOutstandingAsyncRequests;
    private boolean markedInternal;

    public Builder() {

    }

    public Builder(String baseSolrUrl) {
      this.baseSolrUrl = baseSolrUrl;
    }

    public Http2SolrClient build() {
      return new Http2SolrClient(baseSolrUrl, this);
    }

    /**
     * Reuse {@code httpClient} connections pool
     */
    public Builder withHttpClient(Http2SolrClient httpClient) {
      this.http2SolrClient = httpClient;
      return this;
    }

    public Builder withSSLConfig(SSLConfig sslConfig) {
      this.sslConfig = sslConfig;
      return this;
    }

    /**
     * Set maxConnectionsPerHost
     */
    public Builder maxConnectionsPerHost(int max) {
      this.maxConnectionsPerHost = max;
      return this;
    }

    public Builder maxRequestsQueuedPerDestination(int max) {
      this.maxRequestsQueuedPerDestination = max;
      return this;
    }

    public Builder maxThreadPoolSize(int max) {
      this.maxThreadPoolSize = max;
      return this;
    }

    public Builder idleTimeout(int idleConnectionTimeout) {
      this.idleTimeout = idleConnectionTimeout;
      return this;
    }

    public Builder useHttp1_1(boolean useHttp1_1) {
      this.useHttp1_1 = useHttp1_1;
      return this;
    }

    public Builder strictEventOrdering(boolean strictEventOrdering) {
      this.strictEventOrdering = strictEventOrdering;
      return this;
    }

    public Builder connectionTimeout(int connectionTimeOut) {
      this.connectionTimeout = connectionTimeOut;
      return this;
    }

    //do not set this from an external client
    public Builder markInternalRequest() {
      this.headers.put(QoSParams.REQUEST_SOURCE, QoSParams.INTERNAL);
      this.markedInternal = true;
      return this;
    }

    public Builder withBaseUrl(String url) {
      this.baseSolrUrl = url;
      return this;
    }

    public Builder withHeaders(Map<String, String> headers) {
      this.headers.putAll(headers);
      return this;
    }

    public Builder withHeader(String header, String value) {
      this.headers.put(header, value);
      return this;
    }

    public Builder maxOutstandingAsyncRequests(int maxOutstandingAsyncRequests) {
      this.maxOutstandingAsyncRequests = maxOutstandingAsyncRequests;
      return this;
    }
  }

  public Set<String> getQueryParams() {
    return queryParams;
  }

  /**
   * Expert Method
   *
   * @param queryParams set of param keys to only send via the query string
   *                    Note that the param will be sent as a query string if the key is part
   *                    of this Set or the SolrRequest's query params.
   * @see org.apache.solr.client.solrj.SolrRequest#getQueryParams
   */
  public void setQueryParams(Set<String> queryParams) {
    this.queryParams = queryParams;
  }

  private ModifiableSolrParams calculateQueryParams(Set<String> queryParamNames,
                                                    ModifiableSolrParams wparams) {
    ModifiableSolrParams queryModParams = new ModifiableSolrParams();
    if (queryParamNames != null) {
      for (String param : queryParamNames) {
        String[] value = wparams.getParams(param);
        if (value != null) {
          for (String v : value) {
            queryModParams.add(param, v);
          }
          wparams.remove(param);
        }
      }
    }
    return queryModParams;
  }

  public ResponseParser getParser() {
    return parser;
  }

  public void setParser(ResponseParser processor) {
    parser = processor;
  }

  public static void setDefaultSSLConfig(SSLConfig sslConfig) {
    Http2SolrClient.defaultSSLConfig = sslConfig;
  }

  // public for testing, only used by tests
  public static void resetSslContextFactory() {
    Http2SolrClient.defaultSSLConfig = null;
  }

  /* package-private for testing */
  static SslContextFactory.Client getDefaultSslContextFactory() {
    String checkPeerNameStr = System.getProperty(HttpClientUtil.SYS_PROP_CHECK_PEER_NAME);
    boolean sslCheckPeerName = true;
    if (checkPeerNameStr == null || "false".equalsIgnoreCase(checkPeerNameStr)) {
      sslCheckPeerName = false;
    }

    SslContextFactory.Client sslContextFactory = new SslContextFactory.Client(!sslCheckPeerName);

    if (null != System.getProperty("javax.net.ssl.keyStore")) {
      sslContextFactory.setKeyStorePath
          (System.getProperty("javax.net.ssl.keyStore"));
    }
    if (null != System.getProperty("javax.net.ssl.keyStorePassword")) {
      sslContextFactory.setKeyStorePassword
          (System.getProperty("javax.net.ssl.keyStorePassword"));
    }
    if (null != System.getProperty("javax.net.ssl.trustStore")) {
      sslContextFactory.setTrustStorePath
          (System.getProperty("javax.net.ssl.trustStore"));
    }
    if (null != System.getProperty("javax.net.ssl.trustStorePassword")) {
      sslContextFactory.setTrustStorePassword
          (System.getProperty("javax.net.ssl.trustStorePassword"));
    }

    sslContextFactory.setEndpointIdentificationAlgorithm(System.getProperty("solr.jetty.ssl.verifyClientHostName", null));

    return sslContextFactory;
  }

  public static int HEAD(String url, Http2SolrClient httpClient) throws InterruptedException, ExecutionException, TimeoutException {
    ContentResponse response;
    Request req = httpClient.getHttpClient().newRequest(URI.create(url));
    response = req.method(HEAD).send();
    if (response.getStatus() != 200) {
      throw new RemoteSolrException(url, response.getStatus(), response.getReason(), null);
    }
    return response.getStatus();
  }

  public static class SimpleResponse {
    public String asString;
    public String contentType;
    public int size;
    public int status;
    public byte[] bytes;
  }


  public static SimpleResponse DELETE(String url, Http2SolrClient httpClient)
      throws InterruptedException, ExecutionException, TimeoutException {
    return doDelete(url, httpClient, Collections.emptyMap());
  }

  public static SimpleResponse GET(String url)
      throws InterruptedException, ExecutionException, TimeoutException {
    try (Http2SolrClient httpClient = new Http2SolrClient(url, new Http2SolrClient.Builder())) {
      return doGet(url, httpClient, Collections.emptyMap());
    }
  }

  public static SimpleResponse GET(String url, Http2SolrClient httpClient)
          throws InterruptedException, ExecutionException, TimeoutException {
    return doGet(url, httpClient, Collections.emptyMap());
  }

  public static SimpleResponse GET(String url, Http2SolrClient httpClient, Map<String,String> headers)
      throws InterruptedException, ExecutionException, TimeoutException {
    return doGet(url, httpClient, headers);
  }

  public static SimpleResponse POST(String url, Http2SolrClient httpClient, byte[] bytes, String contentType)
          throws InterruptedException, ExecutionException, TimeoutException {
    return doPost(url, httpClient, bytes, contentType, Collections.emptyMap());
  }

  public static SimpleResponse POST(String url, Http2SolrClient httpClient, ByteBuffer bytes, String contentType)
          throws InterruptedException, ExecutionException, TimeoutException {
    return doPost(url, httpClient, bytes, contentType, Collections.emptyMap());
  }

  public static SimpleResponse POST(String url, Http2SolrClient httpClient, ByteBuffer bytes, String contentType, Map<String,String> headers)
          throws InterruptedException, ExecutionException, TimeoutException {
    return doPost(url, httpClient, bytes, contentType, headers);
  }

  public static SimpleResponse PUT(String url, Http2SolrClient httpClient, byte[] bytes, String contentType, Map<String,String> headers)
      throws InterruptedException, ExecutionException, TimeoutException {
    return doPut(url, httpClient, bytes, contentType, headers);
  }

  private static SimpleResponse doGet(String url, Http2SolrClient httpClient, Map<String,String> headers)
          throws InterruptedException, ExecutionException, TimeoutException {
    assert url != null;
    Request req = httpClient.getHttpClient().newRequest(url).method(GET);
    ContentResponse response = req.send();
    SimpleResponse sResponse = new SimpleResponse();
    sResponse.asString = response.getContentAsString();
    sResponse.contentType = response.getEncoding();
    sResponse.size = response.getContent().length;
    sResponse.status = response.getStatus();
    sResponse.bytes = response.getContent();
    return sResponse;
  }

  private static SimpleResponse doDelete(String url, Http2SolrClient httpClient, Map<String,String> headers)
      throws InterruptedException, ExecutionException, TimeoutException {
    assert url != null;
    Request req = httpClient.getHttpClient().newRequest(url).method(DELETE);
    return getSimpleResponse(req);
  }

  public String httpDelete(String url) throws InterruptedException, ExecutionException, TimeoutException {
    ContentResponse response = httpClient.newRequest(URI.create(url)).method(DELETE).send();
    return response.getContentAsString();
  }

  private static SimpleResponse doPost(String url, Http2SolrClient httpClient, byte[] bytes, String contentType,
                                       Map<String,String> headers) throws InterruptedException, ExecutionException, TimeoutException {
    Request req = httpClient.getHttpClient().newRequest(url).method(POST).content(new BytesContentProvider(contentType, bytes));
    headers.forEach(req::header);
    return getSimpleResponse(req);
  }

  private static SimpleResponse doPut(String url, Http2SolrClient httpClient, byte[] bytes, String contentType,
      Map<String,String> headers) throws InterruptedException, ExecutionException, TimeoutException {
    Request req = httpClient.getHttpClient().newRequest(url).method(PUT).content(new BytesContentProvider(contentType, bytes));
    headers.forEach(req::header);
    return getSimpleResponse(req);
  }

  private static SimpleResponse doPost(String url, Http2SolrClient httpClient, ByteBuffer bytes, String contentType,
                                       Map<String,String> headers) throws InterruptedException, ExecutionException, TimeoutException {
    Request req = httpClient.getHttpClient().newRequest(url).method(POST).content(new ByteBufferContentProvider(contentType, bytes));
    headers.forEach(req::header);
    return getSimpleResponse(req);
  }

  private static SimpleResponse getSimpleResponse(Request req) throws InterruptedException, TimeoutException, ExecutionException {
    ContentResponse response = req.send();
    SimpleResponse sResponse = new SimpleResponse();
    sResponse.asString = response.getContentAsString();
    sResponse.contentType = response.getEncoding();
    sResponse.size = response.getContent().length;
    sResponse.status = response.getStatus();
    return sResponse;
  }

  public String httpPut(String url, HttpClient httpClient, byte[] bytes, String contentType)
          throws InterruptedException, ExecutionException, TimeoutException {
    ContentResponse response = httpClient.newRequest(url).method(PUT).content(new BytesContentProvider(bytes), contentType).send();
    return response.getContentAsString();
  }

  private static class SolrHttpClientTransportOverHTTP extends HttpClientTransportOverHTTP {
    public SolrHttpClientTransportOverHTTP(int selectors) {
      super(selectors);
    }

    public HttpClient getHttpClient() {
      return super.getHttpClient();
    }
  }

  private static class MyInputStreamResponseListener extends InputStreamResponseListener {
    private final AsyncListener<InputStream> asyncListener;

    public MyInputStreamResponseListener(HttpClient httpClient, AsyncListener<InputStream> asyncListener) {
      this.asyncListener = asyncListener;
    }

    @Override
    public void onFailure(Response response, Throwable failure) {
      super.onFailure(response, failure);
      try {
        asyncListener.onFailure(new SolrServerException(failure.getMessage(), failure), response.getStatus());
      } catch (Exception e) {
        log.error("Exception in async failure listener", e);
      }
    }
  }

  private class MyFactory implements ConnectionPool.Factory {
    @Override
    public ConnectionPool newConnectionPool(HttpDestination destination) {
      Pool pool = new Pool(Pool.StrategyType.THREAD_ID, getHttpClient().getMaxConnectionsPerDestination(), true);
      pool.setMaxMultiplex(512);

      // MRM TODO: should be configurable
      MultiplexConnectionPool mulitplexPool = new MultiplexConnectionPool(destination, pool, destination,  getHttpClient().getMaxRequestsQueuedPerDestination());
      mulitplexPool.setMaxMultiplex(512);
      mulitplexPool.setMaximizeConnections(false);
      mulitplexPool.preCreateConnections(12);
      return mulitplexPool;
    }
  }
}
