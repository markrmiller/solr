package org.apache.solr.servlet;

import org.agrona.io.ExpandableDirectBufferOutputStream;
import org.apache.commons.io.input.CloseShieldInputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
import org.apache.solr.api.ApiBag;
import org.apache.solr.client.solrj.impl.BaseCloudSolrClient;
import org.apache.solr.client.solrj.impl.BaseHttpSolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.QoSParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.CommandOperation;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.JsonSchemaValidator;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.QueryResponseWriter;
import org.apache.solr.response.QueryResponseWriterUtil;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuditEvent;
import org.apache.solr.security.AuthenticationPlugin;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.AuthorizationResponse;
import org.apache.solr.security.PKIAuthenticationPlugin;
import org.apache.solr.security.PublicKeyHandler;
import org.apache.solr.servlet.cache.Method;
import org.apache.solr.update.processor.DistributingUpdateProcessorFactory;
import org.apache.solr.util.TimeOut;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.client.api.Result;
import org.eclipse.jetty.client.util.InputStreamRequestContent;
import org.eclipse.jetty.client.util.InputStreamResponseListener;
import org.eclipse.jetty.http.HttpField;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.server.HttpOutput;
import org.eclipse.jetty.util.Callback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICATION_FACTOR;
import static org.apache.solr.common.params.CollectionAdminParams.SYSTEM_COLL;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.CREATE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.RELOAD;
import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.common.params.CoreAdminParams.ACTION;
import static org.apache.solr.servlet.SolrDispatchFilter.Action.ADMIN;
import static org.apache.solr.servlet.SolrDispatchFilter.Action.REMOTEQUERY;
import static org.apache.solr.servlet.SolrDispatchFilter.Action.RETRY;
import static org.apache.solr.servlet.SolrDispatchFilter.Action.RETURN;
import javax.annotation.Nonnull;
import javax.servlet.AsyncContext;
import javax.servlet.ServletInputStream;
import javax.servlet.WriteListener;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

public abstract class SolrCall {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final Random random;
  static {
    // We try to make things reproducible in the context of our tests by initializing the random instance
    // based on the current seed
    String seed = System.getProperty("tests.seed");
    if (seed == null) {
      random = new Random();
    } else {
      random = new Random(seed.hashCode());
    }
  }

  private final AtomicReference<List<CommandOperation>> parsedCommands = new AtomicReference<>();

  public static final String ORIGINAL_USER_PRINCIPAL_HEADER = "originalUserPrincipal";

  public String getCt() {
    return ct;
  }

  String ct;

  public abstract SolrDispatchFilter.Action call() throws IOException;

  protected static String extractRemotePath(CoreContainer cores, String collectionName, SolrParams queryParams, HttpServletRequest request) throws SolrException {
    String source = request.getHeader(QoSParams.REQUEST_SOURCE);
    boolean externalRequest = (source == null || !source.equals(QoSParams.INTERNAL));
    if (!externalRequest) {
      return null;
    }

    String coreUrl = getRemoteCoreUrl(cores, collectionName);

    if (coreUrl != null
        && queryParams.get(DistributingUpdateProcessorFactory.DISTRIB_UPDATE_PARAM) == null) {
      //      if (invalidStates != null) {
      //        //it does not make sense to send the request to a remote node
      //        throw new SolrException(SolrException.ErrorCode.INVALID_STATE, new String(Utils.toJSON(invalidStates), org.apache.lucene.util.IOUtils.UTF_8));
      //      }
      return coreUrl;
    }
    return coreUrl;
  }

  protected static void sendError(Throwable ex, HttpServletResponse response) throws IOException {
    log.error("ERROR", ex);
    SimpleOrderedMap info = new SimpleOrderedMap();
    int code = ResponseUtils.getErrorInfo(ex, info, log);
    sendError(code, info.toString(), response);
  }

  protected static void sendError(int code, String message, HttpServletResponse response) throws IOException {
    log.error("ERROR", message);
    try {
      response.sendError(code, message);
    } catch (EOFException e) {
      log.info("Unable to write error response, client closed connection or we are shutting down", e);
    }
  }

  private static String getCoreUrl(CoreContainer cores, Collection<Slice> slices) {
    String coreUrl;
    List<Replica> randomizedReplicas = new ArrayList<>();
    for (Slice slice : slices) {
      randomizedReplicas.clear();
      randomizedReplicas.addAll(slice.getReplicas());
      Collections.shuffle(randomizedReplicas, random);

      for (Replica replica : randomizedReplicas) {
        log.debug("check replica {} with node name {} against live nodes {} with state {}", replica.getName(), replica.getNodeName(),
            cores.getZkController().getZkStateReader().getLiveNodes(), replica.getState());
        if (!replica.getNodeName().equals(cores.getZkController().getNodeName()) && cores.getZkController().zkStateReader.getLiveNodes()
            .contains(replica.getNodeName()) && replica.getState() == Replica.State.ACTIVE) {

          coreUrl = replica.getCoreUrl();

          return coreUrl;
        }
      }
    }

    for (Slice slice : slices) {
      randomizedReplicas.clear();
      randomizedReplicas.addAll(slice.getReplicas());
      Collections.shuffle(randomizedReplicas, random);

      for (Replica replica : randomizedReplicas) {
        log.debug("check replica {} with node name {} against live nodes {} with state {}",
            replica.getName(), replica.getNodeName(), cores.getZkController().getZkStateReader().getLiveNodes(), replica.getState());
        if (!replica.getNodeName().equals(cores.getZkController().getNodeName()) && cores.getZkController().zkStateReader.
            getLiveNodes().contains(replica.getNodeName())) {

          coreUrl = replica.getCoreUrl();

          return coreUrl;
        }
      }
    }

    return null;
  }

  protected static SolrCore getCoreByCollection(CoreContainer cores, String collectionName, boolean isPreferLeader, HttpServletRequest request) {

    String source = request.getHeader(QoSParams.REQUEST_SOURCE);
    boolean externalRequest = (source == null || !source.equals(QoSParams.INTERNAL));
    if (!externalRequest) {
      return null;
    }

    log.debug("get core by collection {} {}", collectionName, isPreferLeader);

    ZkStateReader zkStateReader = cores.getZkController().getZkStateReader();

    DocCollection collection = zkStateReader.getCollectionOrNull(collectionName);

    if (collection == null) {
      log.debug("no local core found for collection={}", collectionName);
      return null;
    }

    if (isPreferLeader) {
      List<Replica> leaderReplicas = collection.getLeaderReplicas(cores.getZkController().getNodeName(), zkStateReader.getLiveNodes());
      log.debug("preferLeader leaderReplicas={}", leaderReplicas);
      SolrCore core = randomlyGetSolrCore(collection, cores, leaderReplicas, true);
      if (core != null) return core;
    }

    List<Replica> replicas = collection.getReplicas(cores.getZkController().getNodeName());
    if (replicas.size() == 0) {
      log.info("replicas={} node={} docReplicas={}", replicas, cores.getZkController().getNodeName(), collection.getReplicas());
    }
    if (log.isDebugEnabled()) log.debug("replicas for node {} {}", replicas, cores.getZkController().getNodeName());
    SolrCore returnCore = randomlyGetSolrCore(collection, cores, replicas, true);
    if (log.isDebugEnabled()) log.debug("returning core by collection {}", returnCore == null ? null : returnCore.getName());
    return returnCore;
  }

  private static SolrCore randomlyGetSolrCore(DocCollection collection, CoreContainer cores, List<Replica> replicas, boolean checkActive) {
    log.info("randomlyGetSolrCore docCollection={} {}", collection, replicas);
    if (replicas != null) {
      HttpSolrCall.RandomIterator<Replica> it = new HttpSolrCall.RandomIterator<>(random, replicas);
      while (it.hasNext()) {
        Replica replica = it.next();

        SolrCore core = checkProps(cores, replica);
        if (core != null && checkActive) {
          if (replica.getState() == Replica.State.ACTIVE) {
            return core;
          }
          try {
            cores.getZkController().getZkStateReader().waitForState(core.getCoreDescriptor().getCollectionName(), 3, TimeUnit.SECONDS, (liveNodes1, coll) -> {
              if (coll == null) {
                return false;
              }
              Replica rep = coll.getReplica(replica.getName());
              if (rep == null) {
                return false;
              }
              return rep.getState() == Replica.State.ACTIVE;
            });
          } catch (InterruptedException e) {
            log.debug("interrupted waiting to see active replica");
            return null;
          } catch (TimeoutException e) {
            log.debug("timeout waiting to see active replica {} {}", replica.getName(), replica.getState());
            return null;
          }
          return core;
        }
      }
    }

    return null;
  }

  /**
   * Resolves the parameter as a potential comma delimited list of collections, and resolves aliases too.
   * One level of aliases pointing to another alias is supported.
   * De-duplicates and retains the order.
   * {@link #getCollectionsList()}
   */
  protected static List<String> resolveCollectionListOrAlias(CoreContainer cores, String collectionStr) {
    if (collectionStr == null || collectionStr.trim().isEmpty()) {
      return Collections.emptyList();
    }
    List<String> result = Collections.emptyList();
    LinkedHashSet<String> uniqueList = null;
    Aliases aliases = getAliases(cores);
    List<String> inputCollections = StrUtils.splitSmart(collectionStr, ",", true);
    if (inputCollections.size() > 1) {
      uniqueList = new LinkedHashSet<>();
    }
    for (String inputCollection : inputCollections) {
      List<String> resolvedCollections = aliases.resolveAliases(inputCollection);
      if (uniqueList != null) {
        uniqueList.addAll(resolvedCollections);
      } else {
        result = resolvedCollections;
      }
    }
    if (uniqueList != null) {
      return List.copyOf(uniqueList);
    } else {
      return Collections.unmodifiableList(result);
    }
  }

  protected static Aliases getAliases(CoreContainer cores) {
    return cores.isZooKeeperAware() ? cores.getZkController().getZkStateReader().getAliases() : Aliases.EMPTY;
  }

  protected static String getRemoteCoreUrl(CoreContainer cores, String collectionName) throws SolrException {
    log.info("getRemoteCoreUrl for {}", collectionName);

    try {
      cores.getZkController().getZkStateReader().waitForState(collectionName, 3, TimeUnit.SECONDS, (liveNodes1, coll) -> {
        if (coll == null) {
          return false;
        }
        for (Slice slice : coll.getSlices()) {

          for (Replica replica : slice.getReplicas()) {
            log.debug("check replica {} with node name {} against live nodes {} with state {}", replica.getName(), replica.getNodeName(),
                cores.getZkController().getZkStateReader().getLiveNodes(), replica.getState());
            if (!replica.getNodeName().equals(cores.getZkController().getNodeName()) && cores.getZkController().zkStateReader.getLiveNodes()
                .contains(replica.getNodeName()) && replica.getState() == Replica.State.ACTIVE) {

              return true;
            }
          }
        }
        return false;
      });
    } catch (Exception e) {

    }

    final DocCollection docCollection = cores.getZkController().getZkStateReader().getCollectionOrNull(collectionName);
    if (docCollection == null) {
      return null;
    }

    String coreUrl = getCoreUrl(cores, docCollection.getSlices());

    //if (log.isDebugEnabled()) {
      log.info("get remote core url returning {} for {}", coreUrl, collectionName);
    //}
    return coreUrl;
  }

  public abstract SolrParams getQueryParams();

  protected void writeResponse(SolrQueryRequest solrReq, SolrQueryResponse solrRsp, HttpServletRequest req, HttpServletResponse response,
      QueryResponseWriter responseWriter, Method reqMethod)
      throws IOException {
    try {
      if (!req.getInputStream().isFinished()) {
        SolrDispatchFilter.consumeInputFully(req, response);
      }

      Map invalidStates = (Map) solrReq.getContext().get(BaseCloudSolrClient.STATE_VERSION);
      //This is the last item added to the response and the client would expect it that way.
      //If that assumption is changed , it would fail. This is done to avoid an O(n) scan on
      // the response for each request
      if (invalidStates != null && invalidStates.size() > 0) {
        solrRsp.add(BaseCloudSolrClient.STATE_VERSION, invalidStates);
      }
      // Now write it out

      // don't call setContentType on null
      if (null != ct) response.setContentType(ct);

      if (solrRsp.getException() != null) {
        NamedList info = new SimpleOrderedMap();
        int code = ResponseUtils.getErrorInfo(solrRsp.getException(), info, log);
        solrRsp.add("error", info);
        response.setStatus(code);
      }


      if (Method.HEAD != reqMethod) {


        ExpandableDirectBufferOutputStream outStream = QueryResponseWriterUtil.writeQueryResponse(responseWriter, solrReq, solrRsp, ct);
        response.setContentLength(outStream.position() - outStream.offset());

        ByteBuffer buffer = outStream.buffer().byteBuffer().asReadOnlyBuffer();
        // buffer.limit(outStream.siz);
        buffer.position(outStream.offset() + outStream.buffer().wrapAdjustment());
        buffer.limit(outStream.position() + outStream.buffer().wrapAdjustment());

        if (SolrDispatchFilter.ASYNC && SolrDispatchFilter.ASYNC_IO) {

          HttpOutput out = (HttpOutput) ((SolrDispatchFilter.CloseShieldHttpServletResponseWrapper) response).response.getOutputStream();
          //out.setWriteListener(new MyWriteListener(out, buffer, req, response));
          out.sendContent(buffer, new Callback() {
            @Override public void succeeded() {
              Callback.super.succeeded();
              req.getAsyncContext().complete();
            }

            @Override public void failed(Throwable t) {
              Callback.super.failed(t);

              log.error("Solr ran into an unexpected problem", t);
              int code = 500;
              if (t instanceof SolrException) {
                code = ((SolrException) t).code();
              }
              try {
              //  response.sendError(code, t.getClass().getName() + ' ' + t.getMessage());
                SolrDispatchFilter.sendException(t, SolrCall.this, req, response);
              } catch (IOException ioException) {
                log.warn("Exception sending error", t); // MRM TODO
              } finally {
                req.getAsyncContext().complete();
              }
            }
          });
        } else {
          HttpOutput out = (HttpOutput) ((SolrDispatchFilter.CloseShieldHttpServletResponseWrapper) response).response.getOutputStream();
          out.sendContent(buffer);
        }
      }
      //else http HEAD request, nothing to write out, waited this long just to get ContentType
    } catch (EOFException e) {
      log.info("Unable to write response, client closed connection or we are shutting down", e);
    }
  }

  private static class MyWriteListener implements WriteListener {
    private final HttpOutput out;
    private final ByteBuffer buffer;
    private final HttpServletRequest req;
    private final HttpServletResponse response;

    public MyWriteListener(HttpOutput out, ByteBuffer buffer, HttpServletRequest req, HttpServletResponse response) {
      this.out = out;
      this.buffer = buffer;
      this.req = req;
      this.response = response;
    }

    @Override public void onWritePossible() throws IOException {
      try {
        while (out.isReady()) {
          if (!buffer.hasRemaining()) {
            log.info("Nothing remaining in BUFFER!");
            // SolrDispatchFilter.consumeInputFully(req, response);
            req.getAsyncContext().complete();
            return;
          }
          log.info("write BUFFER!");
          out.write(buffer);
        }
      } catch (Exception e) {
        log.error("Solr ran into an unexpected problem", e);
        req.getAsyncContext().complete();
      }

    }

    @Override public void onError(Throwable t) {
      log.error("Solr ran into an unexpected problem", t);
      int code = 500;
      if (t instanceof SolrException) {
        code = ((SolrException) t).code();
      }
      try {
        response.sendError(code, t.getClass().getName() + ' ' + t.getMessage());
      } catch (IOException ioException) {
        log.warn("Exception sending error", t); // MRM TODO
      } finally {
        req.getAsyncContext().complete();
      }
    }
  }
  /**
   * Sets the "collection" parameter on the request to the list of alias-resolved collections for this request.
   * It can be avoided sometimes.
   * Note: {@link org.apache.solr.handler.component.HttpShardHandler} processes this param.
   * @see #getCollectionsList()
   */
  protected static void addCollectionParamIfNeeded(CoreContainer cores, SolrQueryRequest solrReq, SolrParams queryParams, List<String> collections) {
    if (collections.isEmpty()) {
      return;
    }
    assert cores.isZooKeeperAware();
    String collectionParam = queryParams.get(COLLECTION_PROP);
    // if there is no existing collection param and the core we go to is for the expected collection,
    //   then we needn't add a collection param
    SolrCore core = solrReq.getCore();
    if (collectionParam == null && // if collection param already exists, we may need to over-write it
        core != null && collections.equals(Collections.singletonList(core.getCoreDescriptor().getCollectionName()))) {
      return;
    }
    String newCollectionParam = StrUtils.join(collections, ',');
    if (newCollectionParam.equals(collectionParam)) {
      return;
    }
    // TODO add a SolrRequest.getModifiableParams ?
    ModifiableSolrParams params = new ModifiableSolrParams(solrReq.getParams());
    params.set(COLLECTION_PROP, newCollectionParam);
    solrReq.setParams(params);
  }

  /** The collection(s) referenced in this request. */
  public abstract List<String> getCollectionsList();

  public abstract AuthorizationContext.RequestType getRequestType();

 
  protected SolrDispatchFilter.Action authorize(CoreContainer cores, SolrQueryRequest solrReq, HttpServletRequest req, HttpServletResponse response) throws IOException {
    AuthorizationContext context = getAuthCtx(solrReq, req, getPath(), getRequestType());
    SolrRequestInfo info = SolrRequestInfo.getRequestInfo();

    log.debug("AuthorizationContext : {}", context);
    AuthorizationResponse authResponse = cores.getAuthorizationPlugin().authorize(context);
    int statusCode = authResponse.statusCode;
    log.info("Authorization response status code {}", authResponse.statusCode);

    if (authResponse.statusCode == 200) {
      response.setHeader(PKIAuthenticationPlugin.HEADER, null);
    }

    if (statusCode == AuthorizationResponse.PROMPT.statusCode) {
      Map<String, String> headers = (Map) req.getAttribute(AuthenticationPlugin.class.getName());
      if (headers != null) {
        headers.forEach(response::setHeader);
      }
      if (log.isDebugEnabled()) {
        log.debug("USER_REQUIRED {} {}", req.getHeader("Authorization"), req.getUserPrincipal());
      }
      sendError(statusCode,
          "Authentication failed, Response code: " + statusCode, response);
      if (shouldAudit(cores ,AuditEvent.EventType.REJECTED)) {
        cores.getAuditLoggerPlugin().doAudit(new AuditEvent(AuditEvent.EventType.REJECTED, req, context));
      }
      return RETURN;
    }
    if (statusCode == AuthorizationResponse.FORBIDDEN.statusCode) {
      if (log.isDebugEnabled()) {
        log.debug("UNAUTHORIZED auth header {} context : {}, msg: {}", req.getHeader("Authorization"), context, authResponse.getMessage());
      }
      sendError(statusCode,
          "Unauthorized request, Response code: " + statusCode, response);
      if (shouldAudit(cores, AuditEvent.EventType.UNAUTHORIZED)) {
        cores.getAuditLoggerPlugin().doAudit(new AuditEvent(AuditEvent.EventType.UNAUTHORIZED, req, context));
      }
      return RETURN;
    }
    if (!(statusCode == HttpStatus.SC_ACCEPTED) && !(statusCode == HttpStatus.SC_OK)) {
      log.warn("ERROR {} during authentication: {}", statusCode, authResponse.getMessage());
      sendError(statusCode,
          "ERROR during authorization, Response code: " + statusCode, response);
      if (shouldAudit(cores, AuditEvent.EventType.ERROR)) {
        cores.getAuditLoggerPlugin().doAudit(new AuditEvent(AuditEvent.EventType.ERROR, req, context));
      }
      return RETURN;
    }
    if (shouldAudit(cores, AuditEvent.EventType.AUTHORIZED)) {
      cores.getAuditLoggerPlugin().doAudit(new AuditEvent(AuditEvent.EventType.AUTHORIZED, req, context));
    }

    return ADMIN;
  }

  public abstract String getPath();

  public abstract void destroy();

  protected static boolean shouldAudit(CoreContainer cores) {
    return cores.getAuditLoggerPlugin() != null;
  }

  protected static boolean shouldAudit(CoreContainer cores, AuditEvent.EventType eventType) {
    return shouldAudit(cores) && cores.getAuditLoggerPlugin().shouldLog(eventType);
  }

  protected static boolean shouldAuthorize(CoreContainer cores, String path, HttpServletRequest req) {
    if(PublicKeyHandler.PATH.equals(path)) return false;
    //admin/info/key is the path where public key is exposed . it is always unsecured
    if ("/".equals(path) || "/solr/".equals(path)) return false; // Static Admin UI files must always be served
    if (cores.getPkiAuthenticationPlugin() != null && req.getUserPrincipal() != null) {
      boolean b = PKIAuthenticationPlugin.needsAuthorization(req);
      log.debug("PkiAuthenticationPlugin says authorization required : {} ", b);
      return b;
    }
    return true;
  }

  protected void handleAdminRequest(CoreContainer cores, HttpServletRequest req, HttpServletResponse response) throws IOException {
    SolrQueryResponse solrResp = new SolrQueryResponse();
    SolrQueryRequest solrReq = getSolrReq();
    SolrCore.preDecorateResponse(solrReq, solrResp);
    handleAdmin(solrResp);
    SolrCore.postDecorateResponse(_getHandler(), solrReq, solrResp);
    //    if (log.isInfoEnabled() && solrResp.getToLog().size() > 0 && !path.startsWith("/admin/metrics")) {
    //      log.info(solrResp.getToLogAsString("[admin]"));
    //    }
    QueryResponseWriter respWriter = SolrCore.DEFAULT_RESPONSE_WRITERS.get(solrReq.getParams().get(CommonParams.WT));
    if (respWriter == null) respWriter = getResponseWriter(solrReq);
    writeResponse(solrReq, solrResp, req, response, respWriter, Method.getMethod(req.getMethod()));
    if (shouldAudit(cores)) {
      AuditEvent.EventType eventType = solrResp.getException() == null ? AuditEvent.EventType.COMPLETED : AuditEvent.EventType.ERROR;
      if (shouldAudit(cores, eventType)) {
        cores.getAuditLoggerPlugin().doAudit(
            new AuditEvent(eventType, req, getAuthCtx(solrReq, req, getPath(), getRequestType()), solrReq.getRequestTimer().getTime(), solrResp.getException()));
      }
    }
  }

  protected abstract void handleAdmin(SolrQueryResponse solrResp);

  private static SolrCore checkProps(CoreContainer cores, Replica replica) {
    SolrCore core = null;
    boolean nodeMatches = cores.getZkController().getNodeName().equals(replica.getNodeName());
    if (nodeMatches) {
      core = cores.getCore(replica.getName());
      if (core == null) {
        while (true) {
          final boolean coreLoading = cores.isCoreLoading(replica.getName());
          if (!coreLoading) break;
          try {
            Thread.sleep(150); // nocommit - make efficient
          } catch (InterruptedException interruptedException) {

          }
        }
        core = cores.getCore(replica.getName());
      }
    }
    log.debug("check local core has correct props replica={} nodename={} nodematches={} core found={}", replica, cores.getZkController().getNodeName(), nodeMatches, core != null);
    return core;
  }

  public abstract SolrQueryRequest getSolrReq();

  /**
   * Returns {@link QueryResponseWriter} to be used.
   * When {@link CommonParams#WT} not specified in the request or specified value doesn't have
   * corresponding {@link QueryResponseWriter} then, returns the default query response writer
   * Note: This method must not return null
   */
  public static QueryResponseWriter getResponseWriter(SolrQueryRequest solrReq) {
    String wt = solrReq.getParams().get(CommonParams.WT);
    SolrCore core = solrReq.getCore();
    if (core != null) {
      return core.getQueryResponseWriter(wt);
    } else {
      return SolrCore.DEFAULT_RESPONSE_WRITERS.getOrDefault(wt,
          SolrCore.DEFAULT_RESPONSE_WRITERS.get("standard"));
    }
  }

  public List<CommandOperation> getCommands(SolrQueryRequest solrReq, boolean validateInput) {

    return parsedCommands.updateAndGet(commandOperations -> {
      if (commandOperations == null) {
        Iterable<ContentStream> contentStreams = null;

        contentStreams = solrReq.getContentStreams();

        if (contentStreams == null) return Collections.EMPTY_LIST;
        else {
          return Collections.unmodifiableList(ApiBag.getCommandOperations(contentStreams.iterator().next(), getValidators(), validateInput));
        }
      }
      return commandOperations;
    });
  }

  protected abstract Map<Object,JsonSchemaValidator> getValidators();

  protected AuthorizationContext getAuthCtx(SolrQueryRequest solrReq, HttpServletRequest req, String path, AuthorizationContext.RequestType requestType) {

    String resource = getPath();

    int idx = resource.indexOf('/');
    int idx2 = -1;

    if (idx > -1) {

      idx2 = resource.indexOf('/', 1);
      if (idx2 > 0) {
        // save the portion after the ':' for a 'handler' path parameter
        resource = resource.substring(idx + 1, idx2);

      } else {
        resource = resource.substring(idx + 1);
        log.debug("core parsed as {}", resource);
      }
    }

    log.info("RESOURCE IS " + resource);

    SolrParams params = getQueryParams();
    final ArrayList<AuthorizationContext.CollectionRequest> collectionRequests = new ArrayList<>();
    for (String collection : getCollectionsList()) {
      collectionRequests.add(new AuthorizationContext.CollectionRequest(collection));
    }

    // Extract collection name from the params in case of a Collection Admin request
    if (resource.equals("/admin/collections")) {
      if (CREATE.isEqual(params.get("action"))||
          RELOAD.isEqual(params.get("action"))||
          DELETE.isEqual(params.get("action")))
        collectionRequests.add(new AuthorizationContext.CollectionRequest(params.get("name")));
      else if (params.get(COLLECTION_PROP) != null)
        collectionRequests.add(new AuthorizationContext.CollectionRequest(params.get(COLLECTION_PROP)));
    }

    // Populate the request type if the request is select or update
    if(requestType == AuthorizationContext.RequestType.UNKNOWN) {
      if(resource.startsWith("/select") || resource.startsWith("/get"))
        requestType = AuthorizationContext.RequestType.READ;
      if(resource.startsWith("/update"))
        requestType = AuthorizationContext.RequestType.WRITE;
    }

    AuthorizationContext.RequestType finalRequestType = requestType;
    String finalResource = resource;
    return new AuthorizationContext() {
      @Override
      public SolrParams getParams() {
        return null == solrReq ? null : solrReq.getParams();
      }

      @Override
      public Principal getUserPrincipal() {
        return req.getUserPrincipal();
      }

      @Override
      public String getHttpHeader(String s) {
        return req.getHeader(s);
      }

      @Override
      public Enumeration<String> getHeaderNames() {
        return req.getHeaderNames();
      }

      @Override
      public List<CollectionRequest> getCollectionRequests() {
        return collectionRequests;
      }

      @Override
      public RequestType getRequestType() {
        return finalRequestType;
      }

      public String getResource() {
        return path;
      }

      @Override
      public String getHttpMethod() {
        return req.getMethod();
      }

      @Override
      public Object getHandler() {
        return _getHandler();
      }

      @Override
      public String toString() {
        StringBuilder response = new StringBuilder("userPrincipal: [").append(getUserPrincipal()).append("]")
            .append(" type: [").append(finalRequestType.toString()).append("], collections: [");
        for (CollectionRequest collectionRequest : collectionRequests) {
          response.append(collectionRequest.collectionName).append(", ");
        }
        if(collectionRequests.size() > 0)
          response.delete(response.length() - 1, response.length());

        response.append("], Path: [").append(finalResource).append("]");
        response.append(" path : ").append(getPath()).append(" params :").append(getParams());
        return response.toString();
      }

      @Override
      public String getRemoteAddr() {
        return req.getRemoteAddr();
      }

      @Override
      public String getRemoteHost() {
        return req.getRemoteHost();
      }
    };

  }

  protected abstract SolrRequestHandler _getHandler();

  protected static SolrDispatchFilter.Action remoteQuery(HttpServletRequest req, HttpServletResponse response, String coreUrl, HttpClient httpClient) throws IOException {
    if (req != null) {
      // MRM TODO: ASYNC IO Mode
      log.info("proxy to:{}?{}", coreUrl, req.getQueryString());
      // MRM TODO: - dont proxy around too much
      String fhost = req.getHeader(HttpHeader.X_FORWARDED_FOR.toString());
      if (fhost != null) {
        // Already proxied
        log.warn("Already proxied, return 404");
        sendError(404, "No SolrCore found to service request.", response);
        return RETURN;
      }

      //System.out.println("protocol:" + req.getProtocol());
      URL url = new URL(coreUrl + '?' + (req.getQueryString() != null ? req.getQueryString() : ""));
      final Request proxyRequest;
      try {
        proxyRequest = httpClient.newRequest(url.toURI()).method(req.getMethod()).version(HttpVersion.fromString(req.getProtocol()));
      } catch (IllegalArgumentException | URISyntaxException e) {
        log.error("Error parsing URI for proxying {}", url, e);
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      }

      copyRequestHeaders(req, proxyRequest);

      addProxyHeaders(req, proxyRequest);

      if (hasContent(req)) {
        ServletInputStream is = req.getInputStream();
        InputStreamRequestContent defferedContent = new InputStreamRequestContent(req.getContentType(), new CloseShieldInputStream(is), 8192);
        //Response.AsyncContentListener content2 = new Response.AsyncContentListener();
        proxyRequest.body(defferedContent);
      }


      if (SolrDispatchFilter.ASYNC) {
        proxyRequest.onResponseHeaders(new RemoteAsyncResponseListener(req, response)).send(new OnContentOutputListener(response, req));
      } else {
        AtomicReference<Throwable> failException = new AtomicReference<>();
        InputStreamResponseListener listener = new RemoteInputStreamResponseListener(failException, response);
        InputStream is = null;
        try {
          proxyRequest.send(listener);

//          try {
//            // wait for headers
//            listener.get(30, TimeUnit.SECONDS);
//          } catch (Exception e) {
//            throw new BaseHttpSolrClient.RemoteSolrException(url.toString(), 0, e.getMessage(), e);
//          }

          try {
            is = listener.getInputStream();
            is.transferTo(response.getOutputStream());
          } catch (Exception e) {
            sendError(e, response);
          }

          if (failException.get() != null) {
            sendError(failException.get(), response);
          }
        } finally {

          if (is != null) {
            while (is.read() != -1) {

            }
          }
        }
      }
    }

    return REMOTEQUERY;
  }

  protected static boolean hasContent(@Nonnull HttpServletRequest clientRequest) {
    return clientRequest.getContentLength() > 0 ||
        clientRequest.getContentType() != null ||
        clientRequest.getHeader(HttpHeader.TRANSFER_ENCODING.asString()) != null;
  }

  protected static void addProxyHeaders(@Nonnull HttpServletRequest clientRequest, Request proxyRequest) {
    proxyRequest.header(HttpHeader.VIA, "HTTP/2.0 Solr Proxy"); //MRM TODO: protocol hard code
    proxyRequest.header(HttpHeader.X_FORWARDED_FOR, clientRequest.getRemoteAddr());
    // we have some tricky to see in tests header size limitations
    // proxyRequest.header(HttpHeader.X_FORWARDED_PROTO, clientRequest.getScheme());
    // proxyRequest.header(HttpHeader.X_FORWARDED_HOST, clientRequest.getHeader(HttpHeader.HOST.asString()));
    // proxyRequest.header(HttpHeader.X_FORWARDED_SERVER, clientRequest.getLocalName());
    proxyRequest.header(QoSParams.REQUEST_SOURCE, QoSParams.INTERNAL);
  }

  protected static void copyRequestHeaders(@Nonnull HttpServletRequest clientRequest, Request proxyRequest) {
    // First clear possibly existing headers, as we are going to copy those from the client request.
    proxyRequest = proxyRequest.headers(httpFields -> httpFields.clear());

    Set<String> headersToRemove = findConnectionHeaders(clientRequest);

    for (Enumeration<String> headerNames = clientRequest.getHeaderNames(); headerNames.hasMoreElements();) {
      String headerName = headerNames.nextElement();
      String lowerHeaderName = headerName.toLowerCase(Locale.ENGLISH);

      boolean preserveHost = false;
      if (HttpHeader.HOST.is(headerName) && !preserveHost)
        continue;

      // Remove hop-by-hop headers.
      if (HOP_HEADERS.contains(lowerHeaderName))
        continue;
      if (headersToRemove != null && headersToRemove.contains(lowerHeaderName))
        continue;

      for (Enumeration<String> headerValues = clientRequest.getHeaders(headerName); headerValues.hasMoreElements();) {
        String headerValue = headerValues.nextElement();
        if (headerValue != null) {
          proxyRequest.headers(httpFields -> httpFields.add(headerName, headerValue));
          //System.out.println("request header: " + headerName + " : " + headerValue);
        }
      }
    }

    // Force the Host header if configured
    // if (_hostHeader != null)
    // proxyRequest.header(HttpHeader.HOST, _hostHeader);
  }

  protected static SolrDispatchFilter.Action autoCreateSystemColl(CoreContainer cores, HttpServletRequest req, String corename) throws Exception {
    if (SYSTEM_COLL.equals(corename) &&
        "POST".equals(req.getMethod()) &&
        !cores.getZkController().getClusterState().hasCollection(SYSTEM_COLL)) {
      log.info("Going to auto-create {} collection", SYSTEM_COLL);
      SolrQueryResponse rsp = new SolrQueryResponse();
      String repFactor = String.valueOf(Math.min(3, cores.getZkController().getZkStateReader().getLiveNodes().size()));
      cores.getCollectionsHandler()
          .handleRequestBody(new LocalSolrQueryRequest(null, new ModifiableSolrParams().add(ACTION, CREATE.toString()).add(NAME, SYSTEM_COLL).add(REPLICATION_FACTOR, repFactor)), rsp);
      if (rsp.getValues().get("success") == null) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Could not auto-create " + SYSTEM_COLL + " collection: " + Utils.toJSONString(rsp.getValues()));
      }
      TimeOut timeOut = new TimeOut(1, TimeUnit.SECONDS, TimeSource.NANO_TIME);
      for (; ; ) {
        if (cores.getZkController().getClusterState().getCollectionOrNull(SYSTEM_COLL) != null) {
          break;
        } else {
          if (timeOut.hasTimedOut()) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Could not find " + SYSTEM_COLL + " collection even after 1 second");
          }
          timeOut.sleep(50);
        }
      }
    }
    return RETRY;
  }


  /** Returns null if the state ({@link CloudSolrClient#STATE_VERSION}) is good; otherwise returns state problems. */
  protected static Map<String, String> checkStateVersionsAreValid(CoreContainer cores, List<String> collectionsList, String stateVer) {
    Map<String, String> result = null;
    String[] pairs;
    if (stateVer != null && !stateVer.isEmpty() && cores.isZooKeeperAware()) {
      // many have multiple collections separated by |
      pairs = StringUtils.split(stateVer, '|');
      for (String pair : pairs) {
        String[] pcs = StringUtils.split(pair, ':');
        if (pcs.length == 2 && !pcs[0].isEmpty() && !pcs[1].isEmpty()) {

          String[] versionAndUpdatesHash = pcs[1].split(">");
          int version = Integer.parseInt(versionAndUpdatesHash[0]);
          int updateHash = Integer.parseInt(versionAndUpdatesHash[1]);

          String status = cores.getZkController().getZkStateReader().compareStateVersions(pcs[0], version, updateHash);
          if (status != null) {
            if (result == null) result = new HashMap<>();
            result.put(pcs[0], status);
          }
        }
      }
    }
    return result;
  }

  static final String CONNECTION_HEADER = "Connection";
  static final String TRANSFER_ENCODING_HEADER = "Transfer-Encoding";
  static final String CONTENT_LENGTH_HEADER = "Content-Length";

  protected static Set<String> findConnectionHeaders(@Nonnull HttpServletRequest clientRequest)
  {
    // Any header listed by the Connection header must be removed:
    // http://tools.ietf.org/html/rfc7230#section-6.1.
    Set<String> hopHeaders = null;
    Enumeration<String> connectionHeaders = clientRequest.getHeaders(HttpHeader.CONNECTION.asString());
    while (connectionHeaders.hasMoreElements())
    {
      String value = connectionHeaders.nextElement();
      String[] values = value.split(",");
      for (String name : values)
      {
        name = name.trim().toLowerCase(Locale.ENGLISH);
        if (hopHeaders == null)
          hopHeaders = new HashSet<>();
        hopHeaders.add(name);
      }
    }
    return hopHeaders;
  }


  protected static final Set<String> HOP_HEADERS;
  static
  {
    //      hopHeaders.add(HttpHeader.X_FORWARDED_FOR.asString());
    //      hopHeaders.add(HttpHeader.X_FORWARDED_PROTO.asString());
    //      hopHeaders.add(HttpHeader.VIA.asString());
    //      hopHeaders.add(HttpHeader.X_FORWARDED_HOST.asString());
    //      hopHeaders.add(HttpHeader.SERVER.asString());
    //
    HOP_HEADERS = Set.of("accept-encoding", "connection", "keep-alive", "proxy-authorization", "proxy-authenticate", "proxy-connection", "transfer-encoding", "te", "trailer", "upgrade");
  }

  private static class RemoteAsyncResponseListener extends InputStreamResponseListener {

    private final HttpServletResponse response;
    private final HttpServletRequest req;

    public RemoteAsyncResponseListener(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse) {
      this.response = httpServletResponse;
      this.req = httpServletRequest;
    }


    @Override
    public void onHeaders(@Nonnull Response resp) {
      super.onHeaders(resp);
      //System.out.println("resp code:" + resp.getStatus());
      for (HttpField field : resp.getHeaders()) {
        String headerName = field.getName();
        String lowerHeaderName = headerName.toLowerCase(Locale.ROOT);
                    System.out.println("response header: " + headerName + " : " + field.getValue() + " status:" +
                            resp.getStatus());
        if (HOP_HEADERS.contains(lowerHeaderName))
          continue;

        response.addHeader(headerName, field.getValue());
      }
      response.setStatus(resp.getStatus());

    }
  }

  private static class ProxyWriteListener implements WriteListener {
    private final HttpOutput out;


    private final ByteBuffer buffer;
    private final HttpServletRequest req;

    public ProxyWriteListener(HttpOutput out, ByteBuffer buffer, HttpServletRequest req) {
      this.out = out;
      this.req = req;
      this.buffer = buffer;
    }

    @Override public void onWritePossible() throws IOException {
      while (out.isReady()) {
        if (!buffer.hasRemaining()) {
          req.getAsyncContext().complete();
          return;
        }
        out.write(buffer);
      }

    }

    @Override public void onError(Throwable t) {
      log.error("Error during remote query", t);
      req.getAsyncContext().complete();
    }
  }


  private static class RemoteInputStreamResponseListener extends InputStreamResponseListener {

    private final AtomicReference<Throwable> failException;
    private final HttpServletResponse response;

    public RemoteInputStreamResponseListener(AtomicReference<Throwable> failException, HttpServletResponse response) {
      this.failException = failException;
      this.response = response;
    }

    public void onComplete(@Nonnull Result result) {
      super.onComplete(result);

      if (result.isFailed()) {
        failException.set(result.getFailure());
      }
    }

    @Override
    public void onHeaders(@Nonnull Response resp) {
      super.onHeaders(resp);
      //System.out.println("resp code:" + resp.getStatus());
      for (HttpField field : resp.getHeaders()) {
        String headerName = field.getName();
        String lowerHeaderName = headerName.toLowerCase(Locale.ROOT);
        //            System.out.println("response header: " + headerName + " : " + field.getValue() + " status:" +
        //                    resp.getStatus());
        if (HOP_HEADERS.contains(lowerHeaderName))
          continue;

        response.addHeader(headerName, field.getValue());
      }
      response.setStatus(resp.getStatus());
      super.onHeaders(resp);
    }
  }

  private static class OnContentOutputListener implements Response.Listener {
    private final HttpServletResponse response;
    private final HttpServletRequest req;

    public OnContentOutputListener(HttpServletResponse response, HttpServletRequest req) {
      this.response = response;
      this.req = req;
    }

    @Override public void onContent(Response r, ByteBuffer buffer) {
      try {
        HttpOutput out = (HttpOutput) ((SolrDispatchFilter.CloseShieldHttpServletResponseWrapper) response).response.getOutputStream();
        ProxyWriteListener proxListener = new ProxyWriteListener(out, buffer, req);
        out.setWriteListener(proxListener);
      } catch (Exception e) {
        log.error("Exception", e);
        req.getAsyncContext().complete();
      }
    }
  }
}
