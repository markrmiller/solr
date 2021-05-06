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
package org.apache.solr.servlet;

import io.opentracing.Span;
import org.apache.solr.client.solrj.impl.BaseCloudSolrClient;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.JsonSchemaValidator;
import org.apache.solr.common.util.ValidatingJsonMap;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.ContentStreamHandlerBase;
import org.apache.solr.handler.admin.PrepRecoveryOp;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.QueryResponseWriter;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuditEvent;
import org.apache.solr.security.AuditEvent.EventType;
import org.apache.solr.security.AuthenticationPlugin;
import org.apache.solr.security.AuthorizationContext.RequestType;
import org.apache.solr.servlet.SolrDispatchFilter.Action;
import org.apache.solr.servlet.cache.HttpCacheHeaderUtil;
import org.apache.solr.servlet.cache.Method;
import org.apache.solr.util.RTimerTree;
import org.apache.solr.util.tracing.GlobalTracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.params.CollectionAdminParams.SYSTEM_COLL;
import static org.apache.solr.servlet.SolrDispatchFilter.Action.ADMIN;
import static org.apache.solr.servlet.SolrDispatchFilter.Action.FORWARD;
import static org.apache.solr.servlet.SolrDispatchFilter.Action.PASSTHROUGH;
import static org.apache.solr.servlet.SolrDispatchFilter.Action.PROCESS;
import static org.apache.solr.servlet.SolrDispatchFilter.Action.REMOTEQUERY;
import static org.apache.solr.servlet.SolrDispatchFilter.Action.RETURN;

/**
 * This class represents a call made to Solr
 **/
public class HttpSolrCall extends SolrCall {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected final SolrDispatchFilter solrDispatchFilter;
  protected final CoreContainer cores;
  protected final HttpServletRequest req;
  protected final HttpServletResponse response;
  private volatile SolrCore solrCore;
  protected SolrQueryRequest solrReq;
  protected final SolrRequestHandler handler;
  protected final SolrParams queryParams;
  protected final String path;
  protected final Action action;
  private final String coreUrl;
  private volatile Map<String,String> invalidStates;


  //The states of client that is invalid in this request
  protected String origCorename; // What's in the URL path; might reference a collection/alias or a Solr core name
  protected List<String> collectionsList; // The list of SolrCloud collections if in SolrCloud (usually 1)

  public RequestType getRequestType() {
    return requestType;
  }

  protected final RequestType requestType;

  public HttpSolrCall(SolrDispatchFilter solrDispatchFilter, String path, HttpServletRequest request, HttpServletResponse response) throws Exception {
    this.solrDispatchFilter = solrDispatchFilter;
    this.cores = solrDispatchFilter.getCores();
    this.req = request;
    this.response = response;
    RequestType requestType = RequestType.UNKNOWN;
    SolrQueryRequest solrRequest = null;
    Action callAction = null;
    request.setAttribute(HttpSolrCall.class.getName(), this);
    queryParams = SolrRequestParsers.getInstance().parseQueryString(request.getQueryString());
    // set a request timer which can be reused by requests if needed
    request.setAttribute(SolrRequestParsers.REQUEST_TIMER_SERVLET_ATTRIBUTE, new RTimerTree());
    // put the core container in request attribute
    request.setAttribute("org.apache.solr.CoreContainer", cores);
    String reqPath = path;
    if (log.isTraceEnabled()) log.trace("Path is parsed as {}", reqPath);


    // check for management path
    if (!cores.isZooKeeperAware()) {
      String alternate = cores.getManagementPath();
      if (alternate != null && reqPath.startsWith(alternate)) {
        reqPath = reqPath.substring(0, alternate.length());
      }
    }

    // Check for container handlers

    SolrRequestHandler reqHandler = cores.getRequestHandler(reqPath);
    if (log.isDebugEnabled()) log.debug("Check for handler {} returned {} handlers={}", reqPath, reqHandler, cores.getRequestHandlers().keySet());
    if (reqHandler != null) {
      solrRequest = SolrRequestParsers.getDefaultInstance().parse(null, reqPath, request);
      solrRequest.getContext().put(CoreContainer.class.getName(), cores);
      this.requestType = RequestType.ADMIN;
      this.handler = reqHandler;
      this.action = ADMIN;
      this.solrReq = solrRequest;
      coreUrl = null;
      this.path = reqPath;
      return;
    }

//    int idx = path.lastIndexOf('/');
//    if (idx > 0) {
//      // save the portion after the ':' for a 'handler' path parameter
//      reqPath = reqPath.substring(0, idx);
//      log.debug("path now {} after removing last /", reqPath);
//    }

    // Parse a core or collection name from the path and attempt to see if it's a core name
    int idx = reqPath.indexOf('/');
    int idx2 = -1;

    if (idx > -1) {

      idx2 = reqPath.indexOf('/', 1);
      if (idx2 > 0) {
        // save the portion after the ':' for a 'handler' path parameter
        origCorename = reqPath.substring(idx + 1, idx2);
        log.debug("core parsed as {}", origCorename);
      } else {
        origCorename = reqPath.substring(idx + 1);
        log.debug("core parsed as {}", origCorename);
      }

      // Try to resolve a Solr core name
      solrCore = cores.getCore(origCorename);

      if (solrCore == null) {
        while (true) {
          final boolean coreLoading = cores.isCoreLoading(origCorename);
          if (!coreLoading) break;
          Thread.sleep(250); // nocommit - make efficient
        }
        solrCore = cores.getCore(origCorename);
      }

      //?[

      if (solrCore == null && log.isDebugEnabled()) {
        log.debug("tried to get core by name {} got {}, loaded cores {}", origCorename, solrCore, cores.getLoadedCoreNames());
      }

      if (solrCore != null) {
        if (idx2 > 0) {
          reqPath = reqPath.substring(idx2);
        }
        if (log.isDebugEnabled()) log.debug("Path is parsed as {}", reqPath);
      } else {
        if (!cores.isZooKeeperAware()) {
          solrCore = cores.getCore("");
        }
      }
    }

    if (solrCore == null && cores.isZooKeeperAware()) {
      // init collectionList (usually one name but not when there are aliases)
      String def = origCorename;
      collectionsList = resolveCollectionListOrAlias(cores, queryParams.get(COLLECTION_PROP, def)); // &collection= takes precedence

      // lookup core from collection, or route away if need to
      String collectionName = collectionsList.isEmpty() ? null : collectionsList.get(0); // route to 1st
      //TODO try the other collections if can't find a local replica of the first?   (and do to V2HttpSolrCall)

      boolean isPreferLeader = (reqPath.endsWith("/update") || reqPath.contains("/update/"));

      if (SYSTEM_COLL.equals(collectionName)) {
        autoCreateSystemColl(cores, request, collectionName);
      }
      if (collectionName != null) {
        solrCore = getCoreByCollection(cores, collectionName, isPreferLeader, request); // find a local replica/core for the collection
      }
      if (solrCore != null) {
        if (idx2 > 0) {
          reqPath = reqPath.substring(idx2);
        }
        if (log.isDebugEnabled()) log.debug("Path is parsed as {}", reqPath);
      } else {
        // if we couldn't find it locally, look on other nodes
        if (log.isDebugEnabled()) log.debug("check remote path extraction {} {}", collectionName, origCorename);

        // don't proxy for internal update requests
        invalidStates = checkStateVersionsAreValid(cores, getCollectionsList(), queryParams.get(BaseCloudSolrClient.STATE_VERSION));

        String coreUrl = null;

        if (origCorename != null) {
          coreUrl = extractRemotePath(cores, origCorename, queryParams, request);
        }

        if (coreUrl != null) {
          if (idx2 > 0) {
            reqPath = reqPath.substring(idx2);
          }
          if (log.isDebugEnabled()) log.debug("Path is parsed as {}", reqPath);
          solrReq = null;
          handler = null;
          this.path = reqPath;
          this.requestType = requestType;
          action = REMOTEQUERY;
          this.coreUrl = coreUrl;
          return;
        } else if (collectionName != null){
          coreUrl = extractRemotePath(cores, collectionName, queryParams, request);
          if (coreUrl != null) {
            if (idx2 > 0) {
              reqPath = reqPath.substring(idx2);
            }
            if (log.isDebugEnabled()) log.debug("Path is parsed as {}", reqPath);
            this.path = reqPath;
            solrReq = null;
            handler = null;
            this.requestType = requestType;
            action = REMOTEQUERY;
            this.coreUrl = coreUrl;
            return;
          }
        }
      }

      //core is not available locally or remotely

    }

    // With a valid core...
    if (solrCore != null) {
      // get or create/cache the parser for the core
      SolrRequestParsers parser = solrCore.getSolrConfig().getRequestParsers();

      // Determine the handler from the url path if not set
      // (we might already have selected the cores handler)

      log.debug("Extract handler from url path {}", reqPath);


      if (reqPath.length() > 1) { // don't match "" or "/" as valid path
        reqHandler = solrCore.getRequestHandler(reqPath);

        log.debug("core={} handler={}", solrCore, reqPath);

        // no handler yet but <requestDispatcher> allows us to handle /select with a 'qt' param
        if (reqHandler == null && parser.isHandleSelect()) {
          if ("/select".equals(reqPath) || "/select/".equals(reqPath)) {
            this.solrReq = parser.parse(solrCore, reqPath, request);
            SolrParams params = this.solrReq.getParams();
            String qt = params.get(CommonParams.QT);
            reqHandler = solrCore.getRequestHandler(qt);
            if (reqHandler == null) {
              throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "unknown handler: " + qt);
            }
            if (qt != null && !qt.isEmpty() && qt.charAt(0) == '/' && (reqHandler instanceof ContentStreamHandlerBase)) {
              //For security reasons it's a bad idea to allow a leading '/', ex: /select?qt=/update see SOLR-3161
              //There was no restriction from Solr 1.4 thru 3.5 and it's not supported for update handlers.
              throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Invalid Request Handler ('qt').  Do not use /select to access: " + qt);
            }
          }
        }
      }

      // With a valid handler and a valid core...
      if (this.solrReq == null) {
        // if not a /select, create the request
        log.debug("build solrequest from handler={}", reqPath);
        this.solrReq = parser.parse(solrCore, reqPath, request);

        log.debug("handler={} result solrReq={}", reqHandler, solrReq);

        invalidStates = checkStateVersionsAreValid(cores, getCollectionsList(), queryParams.get(BaseCloudSolrClient.STATE_VERSION));

        addCollectionParamIfNeeded(cores, solrReq, queryParams, getCollectionsList());
        this.handler = reqHandler;
        coreUrl = null;
        this.action = PROCESS;
        this.requestType = requestType;
        this.path = reqPath;
        return; // we are done with a valid handler
      } else {
        if (request.getMethod().equals("HEAD")) {
          this.action = RETURN;
          solrReq = null;
          handler = null;
          this.requestType = requestType;
          this.path = reqPath;
          coreUrl = null;
          return;
        }
      }
    }
    log.debug("no handler or core retrieved for {}, follow through...", reqPath);

    callAction = PASSTHROUGH;
    this.handler = null;
    this.action = callAction;
    this.path = reqPath;
    this.requestType = requestType;
    coreUrl = null;
    this.solrReq = null;
  }

  public String getPath() {
    return path;
  }

  public HttpServletRequest getReq() {
    return req;
  }

  @Override
  public SolrQueryRequest getSolrReq() {
    return solrReq;
  }

  public SolrParams getQueryParams() {
    return queryParams;
  }

  /** The collection(s) referenced in this request. */
  @Override
  public List<String> getCollectionsList() {
    return collectionsList != null ? collectionsList : Collections.emptyList();
  }

  /**
   * This method processes the request.
   */
  public Action call() throws IOException {
    MDCLoggingContext.reset();
    Span activeSpan = GlobalTracer.getTracer().activeSpan();
    if (activeSpan != null) {
      MDCLoggingContext.setTracerId(activeSpan.context().toTraceId());
    }
    if (cores.isZooKeeperAware()) {
      MDCLoggingContext.setNode(cores.getZkController().getNodeName());
    }

    SolrQueryResponse solrRsp = null;
    QueryResponseWriter responseWriter = null;
    if (solrReq != null) {
      solrRsp = new SolrQueryResponse();
      responseWriter = getResponseWriter(solrReq);
      if (responseWriter != null) {
        ct = responseWriter.getContentType(solrReq, solrRsp);
      }
    }

    if (solrDispatchFilter.abortErrorMessage != null) {
      sendError(500, solrDispatchFilter.abortErrorMessage, response);
      if (shouldAudit(cores, EventType.ERROR)) {
        cores.getAuditLoggerPlugin().doAudit(new AuditEvent(EventType.ERROR, req));
      }
      return RETURN;
    }

    try {

      // Perform authorization here, if:
      //    (a) Authorization is enabled, and
      //    (b) The requested resource is not a known static file
      //    (c) And this request should be handled by this node (see NOTE below)
      // NOTE: If the query is to be handled by another node, then let that node do the authorization.
      // In case of authentication using BasicAuthPlugin, for example, the internode request
      // is secured using PKI authentication and the internode request here will contain the
      // original user principal as a payload/header, using which the receiving node should be
      // able to perform the authorization.
      if (cores.getAuthorizationPlugin() != null && shouldAuthorize(cores, path, req)
          && !(action == REMOTEQUERY || action == FORWARD)) {
        Action authorizationAction = authorize(cores, solrReq, req, response);
        if (authorizationAction == RETURN) {
          return authorizationAction;
        }
        if (authorizationAction == ADMIN) {
          handleAdminRequest(cores, req, response);
          return RETURN;
        }
      }

      HttpServletResponse resp = response;
      switch (action) {
        case ADMIN:
          handleAdminRequest(cores, req, response);
          return RETURN;
        case REMOTEQUERY:
          SolrRequestInfo.setRequestInfo(new SolrRequestInfo(req, new SolrQueryResponse(), action));
          Action a = remoteQuery(req, response, coreUrl + path, solrDispatchFilter.getCores().getUpdateShardHandler().getTheSharedHttpClient().getHttpClient());
          return a;
        case PROCESS:
          final Method reqMethod = Method.getMethod(req.getMethod());
          SolrConfig config = solrReq.getCore().getSolrConfig();
          HttpCacheHeaderUtil.setCacheControlHeader(config, resp, reqMethod);
          // unless we have been explicitly told not to, do cache validation
          // if we fail cache validation, execute the query
          if (config.getHttpCachingConfig().isNever304() ||
              !HttpCacheHeaderUtil.doCacheHeaderValidation(solrReq, req, reqMethod, resp)) {

              /* even for HEAD requests, we need to execute the handler to
               * ensure we don't get an error (and to make sure the correct
               * QueryResponseWriter is selected and we get the correct
               * Content-Type)
               */
            SolrRequestInfo.setRequestInfo(new SolrRequestInfo(solrReq, solrRsp, action));
            execute(solrRsp);
            if (shouldAudit(cores)) {
              EventType eventType = solrRsp.getException() == null ? EventType.COMPLETED : EventType.ERROR;
              if (shouldAudit(cores, eventType)) {
                cores.getAuditLoggerPlugin().doAudit(
                    new AuditEvent(eventType, req, getAuthCtx(solrReq, req, path, requestType), solrReq.getRequestTimer().getTime(), solrRsp.getException()));
              }
            }
            HttpCacheHeaderUtil.checkHttpCachingVeto(solrRsp, resp, reqMethod);
            Iterator<Map.Entry<String, String>> headers = solrRsp.httpHeaders();
            while (headers.hasNext()) {
              Map.Entry<String, String> entry = headers.next();
              resp.addHeader(entry.getKey(), entry.getValue());
            }

            if (invalidStates != null) solrReq.getContext().put(BaseCloudSolrClient.STATE_VERSION, invalidStates);
            writeResponse(solrReq, solrRsp, req, response, responseWriter, reqMethod);
          }
          return RETURN;
        default: return action;
      }
    } catch (Throwable ex) {
      log.error("ERROR", ex);
      if (!(ex instanceof PrepRecoveryOp.NotValidLeader) && shouldAudit(cores, EventType.ERROR)) {
        cores.getAuditLoggerPlugin().doAudit(new AuditEvent(EventType.ERROR, ex, req));
      }
      // walk the the entire cause chain to search for an Error
      Throwable t = ex;
      while (t != null) {
        if (t instanceof Error) {
          if (t != ex) {
            log.error("An Error was wrapped in another exception - please report complete stacktrace on SOLR-6161", ex);
          }
          throw (Error) t;
        }
        t = t.getCause();
      }
      throw ex;
    }

  }

  public void destroy() {
    try {
      if (solrReq != null) {
        if (log.isTraceEnabled()) {
          log.trace("Closing out SolrRequest: {}", solrReq);
        }

        IOUtils.closeQuietly(solrCore);

        IOUtils.closeQuietly(solrReq);
      }
    } finally {
      try {
        AuthenticationPlugin authcPlugin = cores.getAuthenticationPlugin();
        if (authcPlugin != null) authcPlugin.closeRequest();
      } finally {
        SolrRequestInfo.clearRequestInfo();
      }
    }
  }

  @Override
  protected SolrRequestHandler _getHandler() {
    return handler;
  }

  protected void execute(SolrQueryResponse rsp) {
    // a custom filter could add more stuff to the request before passing it on.
    // for example: sreq.getContext().put( "HttpServletRequest", req );
    // used for logging query stats in SolrCore.execute()
    log.debug("execute solrReq={}, req={}", solrReq, req);
 //   solrReq.getContext().put("webapp", req.getContextPath());
    solrReq.getCore().execute(handler, solrReq, rsp);
  }

  protected void handleAdmin(SolrQueryResponse solrResp) {
    handler.handleRequest(solrReq, solrResp);
  }

  protected static ValidatingJsonMap getSpec() {
    return null;
  }

  @Override
  protected Map<Object,JsonSchemaValidator> getValidators(){
    return Collections.EMPTY_MAP;
  }

  /**
   * A faster method for randomly picking items when you do not need to
   * consume all items.
   */
  static class RandomIterator<E> implements Iterator<E> {
    private Random rand;
    private ArrayList<E> elements;
    private int size;

    public RandomIterator(Random rand, Collection<E> elements) {
      this.rand = rand;
      this.elements = new ArrayList<>(elements);
      this.size = elements.size();
    }

    @Override
    public boolean hasNext() {
      return size > 0;
    }

    @Override
    public E next() {
      int idx = rand.nextInt(size);
      E e1 = elements.get(idx);
      E e2 = elements.get(size-1);
      elements.set(idx,e2);
      size--;
      return e1;
    }
  }

}
