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
package org.apache.solr.client.solrj.embedded;

import com.codahale.metrics.ScheduledReporter;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.cloud.SocketProxy;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.SolrQTP;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.servlet.SolrDispatchFilter;
import org.apache.solr.servlet.SolrQoSFilter;
import org.eclipse.jetty.alpn.server.ALPNServerConnectionFactory;
import org.eclipse.jetty.http2.HTTP2Cipher;
import org.eclipse.jetty.http2.server.HTTP2CServerConnectionFactory;
import org.eclipse.jetty.http2.server.HTTP2ServerConnectionFactory;
import org.eclipse.jetty.rewrite.handler.RewriteHandler;
import org.eclipse.jetty.rewrite.handler.RewritePatternRule;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.server.handler.HandlerWrapper;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.Source;
import org.eclipse.jetty.util.component.LifeCycle;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import javax.servlet.DispatcherType;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.BindException;
import java.net.URI;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

/**
 * Run solr using jetty
 *
 * @since solr 1.3
 */
public class JettySolrRunner implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public final static boolean ASYNC = true;

  public static final Logger metricsLog = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass().getName() + ".Metrics");

  final Server server;
  private final ServerConnector connector;

  volatile FilterHolder dispatchFilter;
  volatile FilterHolder debugFilter;
  volatile FilterHolder qosFilter;

  private volatile int jettyPort = -1;

  private final JettyConfig config;
  private final String solrHome;
  private final Properties nodeProperties;

  private volatile boolean startedBefore = false;

  private static final String excludePatterns = "/partials/.+|/libs/.+|/css/.+|/js/.+|/img/.+|/templates/.+|/tpl/.+";

  private volatile int proxyPort = -1;

  private final boolean enableProxy;

  private SocketProxy proxy;

  private String protocol;

  private volatile String host;

  private volatile boolean started = false;
  private volatile String nodeName;

  private volatile boolean closed;
  private ScheduledReporter reporter;

  public String getContext() {
    return config.context;
  }

  public static class DebugFilter implements Filter {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final LongAdder nRequests = new LongAdder();

    private final Set<Delay> delays = ConcurrentHashMap.newKeySet(12);

    public long getTotalRequests() {
      return nRequests.longValue();

    }
    
    /**
     * Introduce a delay of specified milliseconds for the specified request.
     *
     * @param reason Info message logged when delay occurs
     * @param count The count-th request will experience a delay
     * @param delay There will be a delay of this many milliseconds
     */
    public void addDelay(String reason, int count, int delay) {
      delays.add(new Delay(reason, count, delay));
    }
    
    /**
     * Remove any delay introduced before.
     */
    public void unsetDelay() {
      delays.clear();
    }


    @Override
    public void init(FilterConfig filterConfig) throws ServletException { }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {
      nRequests.increment();
      executeDelay();
      filterChain.doFilter(servletRequest, servletResponse);
    }

    @Override
    public void destroy() { }

    private void executeDelay() {
      int delayMs = 0;
      for (Delay delay: delays) {
        log.info("Delaying {}, for reason: {}", delay.delayValue, delay.reason);
        if (delay.counter.decrementAndGet() == 0) {
          delayMs += delay.delayValue;
        }
      }

      if (delayMs > 0) {
        log.info("Pausing this socket connection for {}ms...", delayMs);
        try {
          Thread.sleep(delayMs);
        } catch (InterruptedException e) {

        }
        log.info("Waking up after the delay of {}ms...", delayMs);
      }
    }

  }

  /**
   * Create a new JettySolrRunner.
   *
   * After construction, you must start the jetty with {@link #start()}
   *
   * @param solrHome the solr home directory to use
   * @param context the context to run in
   * @param port the port to run on
   */
  public JettySolrRunner(String solrHome, String context, int port) {
    this(solrHome, JettyConfig.builder().setContext(context).setPort(port).build());
  }


  /**
   * Construct a JettySolrRunner
   *
   * After construction, you must start the jetty with {@link #start()}
   *
   * @param solrHome    the base path to run from
   * @param config the configuration
   */
  public JettySolrRunner(String solrHome, JettyConfig config) {
    this(solrHome, new Properties(), config);
  }

  /**
   * Construct a JettySolrRunner
   *
   * After construction, you must start the jetty with {@link #start()}
   *
   * @param solrHome            the solrHome to use
   * @param nodeProperties      the container properties
   * @param config         the configuration
   */
  public JettySolrRunner(String solrHome, Properties nodeProperties, JettyConfig config) {
    assert ObjectReleaseTracker.getInstance().track(this);

    this.enableProxy = config.enableProxy;
    this.solrHome = solrHome;
    this.config = config;
    this.nodeProperties = nodeProperties;
    nodeProperties.setProperty("hostContext", config.context);

    if (enableProxy) {
      try {
        proxy = new SocketProxy(0, config.sslConfig != null && config.sslConfig.isSSLMode());
      } catch (Exception e) {
        ParWork.propagateInterrupt(e);
        throw new RuntimeException(e);
      }
      proxyPort = proxy.getListenPort();
    }

    int port = this.config.port;

    // leave as match with prod setup
    //qtp = Objects.requireNonNullElseGet(config.qtp, () -> new SolrQueuedThreadPool("JettySolrRunner qtp"));

    //qtp = new SolrQueuedThreadPool("JettySolrRunner qtp");
    //qtp = new QueuedThreadPool(Integer.getInteger("solr.jettyRunnerThreadPoolMaxSize", 200), 24);
    QueuedThreadPool qtp = new SolrQTP("jetty", Integer.getInteger("solr.jettyRunnerThreadPoolMaxSize", 18), 12);

    //qtp = new SolrQTP("jetty", Integer.getInteger("solr.jettyRunnerThreadPoolMaxSize", 60), 8, new MPMCQueue.RunnableBlockingQueue());
    //ParWork.getQueue())
    //qtp = new QueuedThreadPool(300, 120);

    server = new Server(qtp);
    server.manage(qtp);

//    if (config.qtp != null) {
//      server.unmanage(config.qtp);
//    }



//    Configuration.ClassList classlist = Configuration.ClassList
//        .setServerDefault(server);
//
//    classlist.addBefore(
//        "org.eclipse.jetty.webapp.JettyWebXmlConfiguration",
//        "org.eclipse.jetty.annotations.AnnotationConfiguration");
    server.setStopAtShutdown(true);
    server.setStopTimeout(5000); // will wait gracefully for stoptime / 2, then interrupts



    long idleTimeout = TimeUnit.SECONDS.toMillis(30);

    //if (System.getProperty("jetty.testMode") != null) {

    // if this property is true, then jetty will be configured to use SSL
    // leveraging the same system properties as java to specify
    // the keystore/truststore if they are set unless specific config
    // is passed via the constructor.
    //
    // This means we will use the same truststore, keystore (and keys) for
    // the server as well as any client actions taken by this JVM in
    // talking to that server, but for the purposes of testing that should
    // be good enough
    SslContextFactory.Server sslContextFactory = SSLConfig.createContextFactory(config.sslConfig);

    HttpConfiguration httpConfig = new HttpConfiguration();
    httpConfig.setRequestHeaderSize(16 << 10);
    httpConfig.setResponseHeaderSize(16 << 10);

    // https://github.com/jersey/jersey/issues/3691
    // https://github.com/eclipse/jetty.project/issues/1891
    httpConfig.setNotifyRemoteAsyncErrors(true);

    //server.setRequestLog(new AsyncNCSARequestLog());

    httpConfig.setIdleTimeout(idleTimeout);


    if (config.onlyHttp1) {
      HttpConnectionFactory http1ConnectionFactory = new HttpConnectionFactory(httpConfig);
      // ParWork.getRootSharedIOExecutor()
      connector = new ServerConnector(server, null, null, null, 1, 1, new SslConnectionFactory(sslContextFactory, http1ConnectionFactory.getProtocol()),
          http1ConnectionFactory);
    } else {


      // HTTPS Configuration
      HttpConfiguration httpsConfig = new HttpConfiguration(httpConfig);
      //  httpsConfig.addCustomizer(new SecureRequestCustomizer());

      SslConnectionFactory ssl = null;

      ALPNServerConnectionFactory alpn = null;

      if (sslContextFactory != null) {

        alpn = new ALPNServerConnectionFactory();
        alpn.setDefaultProtocol("h2");

        // HTTP Configuration
        httpConfig.setSecureScheme("https");
        //   httpConfig.setSecurePort(port + 1000);
        httpConfig.setSendServerVersion(false);

        // SSL Context Factory for HTTPS and HTTP/2

        sslContextFactory.setCipherComparator(HTTP2Cipher.COMPARATOR);
        sslContextFactory.setUseCipherSuitesOrder(true);

        // SSL Connection Factory
        ssl = new SslConnectionFactory(sslContextFactory, alpn.getProtocol());
      }

      HttpConnectionFactory httpFactory;
      // HTTP/2 Connection Factory
      HTTP2ServerConnectionFactory h2;
      if (sslContextFactory == null) {
        httpFactory = new HttpConnectionFactory(httpConfig);
        h2 = new HTTP2CServerConnectionFactory(httpConfig);
      } else {
        httpFactory = new HttpConnectionFactory(httpsConfig);
        h2 = new HTTP2ServerConnectionFactory(httpsConfig);
      }
      h2.setMaxConcurrentStreams(512);
      h2.setInputBufferSize(8192);
      //h2.setStreamIdleTimeout(idleTimeout);

    //  NegotiatingServerConnectionFactory.checkProtocolNegotiationAvailable();


      // HTTP/2 Connector
      if (ssl == null) {
        connector = new ServerConnector(server, ParWork.getRootSharedIOExecutor(), null, null, 1, 1, httpFactory, h2);
      } else {
        connector = new ServerConnector(server, ParWork.getRootSharedIOExecutor(), null, null, 1, 1, ssl, alpn, h2, httpFactory);
        alpn.setDefaultProtocol(httpFactory.getProtocol());
      }

    }

    connector.setReuseAddress(true);
    connector.setPort(port);
    connector.setHost("127.0.0.1");

    server.addConnector(connector);
    server.manage(connector);

//    server.setDumpAfterStart(true);
//    server.setDumpBeforeStop(true);

    HandlerWrapper chain;
    {
      // Initialize the servlets
     final ServletContextHandler root = new ServletContextHandler(server, config.context, ServletContextHandler.NO_SESSIONS);

      //root.addServlet(new ServletHolder(new MetricsServlet()), "/metrics");

      root.getServletContext().setAttribute(SolrDispatchFilter.PROPERTIES_ATTRIBUTE, nodeProperties);
      root.getServletContext().setAttribute(SolrDispatchFilter.SOLRHOME_ATTRIBUTE, solrHome);
      root.getServletContext().setAttribute(SolrDispatchFilter.LOAD_CORES, false);

      debugFilter = root.addFilter(DebugFilter.class, "*", EnumSet.of(DispatcherType.REQUEST));
      //debugFilter.setAsyncSupported(true);
      config.extraFilters.forEach((key, value) -> root.addFilter(key, value, EnumSet.of(DispatcherType.REQUEST)));

      config.extraServlets.forEach(root::addServlet);

      qosFilter = root.getServletHandler().newFilterHolder(Source.EMBEDDED);
      qosFilter.setHeldClass(SolrQoSFilter.class);
      qosFilter.setAsyncSupported(true);

      dispatchFilter = root.getServletHandler().newFilterHolder(Source.EMBEDDED);

      dispatchFilter.setHeldClass(SolrDispatchFilter.class);
      dispatchFilter.setInitParameter("excludePatterns", excludePatterns);

      if (ASYNC) {
        dispatchFilter.setAsyncSupported(true);
      }


      // Map dispatchFilter in same path as in web.xml

      root.addFilter(qosFilter, "*", EnumSet.of(DispatcherType.REQUEST, DispatcherType.ASYNC));

      if (ASYNC) {
        root.addFilter(dispatchFilter, "*", EnumSet.of(DispatcherType.REQUEST, DispatcherType.ASYNC));
      } else {
        root.addFilter(dispatchFilter, "*", EnumSet.of(DispatcherType.REQUEST));
      }

      root.addServlet(Servlet404.class, "/*");

      if (log.isDebugEnabled()) log.debug("Jetty loaded and ready to go");

      server.addEventListener(new LifeCycle.Listener() {

        @Override
        public void lifeCycleStopping(LifeCycle arg0) {
        }

        @Override
        public void lifeCycleStopped(LifeCycle arg0) {
        }

        @Override
        public void lifeCycleStarting(LifeCycle arg0) {

          if (startedBefore) {
            root.getServletContext().setAttribute(SolrDispatchFilter.PROPERTIES_ATTRIBUTE, nodeProperties);
            root.getServletContext().setAttribute(SolrDispatchFilter.SOLRHOME_ATTRIBUTE, solrHome);
            root.getServletContext().setAttribute(SolrDispatchFilter.LOAD_CORES, false);
          }

        }

        @Override
        public void lifeCycleStarted(LifeCycle arg0) {


          jettyPort = getFirstConnectorPort();


          root.getServletContext().setAttribute(SolrDispatchFilter.INIT_CALL, (Runnable) () -> {
            int port1 = jettyPort;
            if (proxyPort != -1) port1 = proxyPort;
            nodeProperties.setProperty("hostPort", String.valueOf(port1));

          });

          CoreContainer cores = ((CoreContainer) root.getServletContext().getAttribute("cores"));

          if (cores.getNodeConfig().getCloudConfig() != null) {
            cores.getNodeConfig().getCloudConfig().setPort(jettyPort);
          }

          cores.load();

          boolean enableMetrics = Boolean.parseBoolean(System.getProperty("solr.enableMetrics", "true"));
          if (enableMetrics) {
            ParWork.getRootSharedIOExecutor().submit(() -> SolrDispatchFilter.setupJvmMetrics(cores));
          }

          if (log.isDebugEnabled()) {
            log.debug("user.dir={}", System.getProperty("user.dir"));
          }



         // SolrMetricManager metricsManager = cores.getMetricManager();

         // MetricRegistry metricsRegisty = metricsManager.registry("solr.jetty");

          //MetricRegistry metricsRegisty = Metrics.MARKS_METRICS;

//          reporter = Slf4jReporter.forRegistry(metricsRegisty)
//              .convertRatesTo(TimeUnit.SECONDS)
//              .convertDurationsTo(TimeUnit.MILLISECONDS)
//              .outputTo(metricsLog)
//              .build();
//
//          reporter = GraphiteReporter.forRegistry(metricsRegisty)
//              .convertRatesTo(TimeUnit.SECONDS)
//              .convertDurationsTo(TimeUnit.MILLISECONDS)
//              .build(new GraphiteUDP("127.0.0.1", 2003));
//
//          reporter.start(3, TimeUnit.SECONDS);

       //   CollectorRegistry.defaultRegistry.register(new DropwizardExports(metricsRegisty));

//          String apiKey = System.getProperty("solr.newrelic.apikey", null);
//          if (apiKey != null) {
//            MetricBatchSender metricBatchSender = SimpleMetricBatchSender.builder(apiKey).build();
//
//            Attributes commonAttributes = null;
//            try {
//              commonAttributes = new Attributes().put("host", InetAddress.getLocalHost().getHostName()).put("appName", "Solr");
//            } catch (UnknownHostException e) {
//              log.error("Could not get hostname for newrelica reporter", e);
//            }
//
//            reporter = NewRelicReporter.build(metricsRegisty, metricBatchSender).commonAttributes(commonAttributes).build();
//
//            reporter.start(3,10, TimeUnit.SECONDS);
//          }

      //    SolrPaths.ensureUserFilesDataDir(solrHomePath);

          if (log.isDebugEnabled()) {
            log.debug("user.dir={}", System.getProperty("user.dir"));
          }


          if (!startedBefore) {
            startedBefore = true;
          }

        }

        @Override
        public void lifeCycleFailure(LifeCycle arg0, Throwable arg1) {
          System.clearProperty("hostPort");
        }
      });
      // Default servlet as a fall-through
      root.addServlet(Servlet404.class, "/");
      chain = root;
    }

    // no listener for jettysolrrunner, it requires one jetty per jvm
    //server.addLifeCycleListener(new SolrLifcycleListener());

    chain = injectJettyHandlers(chain);

    if(config.enableV2) {
      RewriteHandler rwh = new RewriteHandler();
      rwh.setHandler(chain);
      rwh.setRewriteRequestURI(true);
      rwh.setRewritePathInfo(false);
      rwh.setDispatcherTypes(EnumSet.of(DispatcherType.REQUEST, DispatcherType.ASYNC));
      rwh.setOriginalPathAttribute("requestedPath");
      rwh.addRule(new RewritePatternRule("/api/*", "/solr/____v2"));
      chain = rwh;
    }
//    GzipHandler gzipHandler = new GzipHandler();
//    gzipHandler.setHandler(chain);
//
//    gzipHandler.setMinGzipSize(23); // https://github.com/eclipse/jetty.project/issues/4191
//    gzipHandler.setCheckGzExists(false);
//    gzipHandler.setCompressionLevel(-1);
//    gzipHandler.setExcludedAgentPatterns(".*MSIE.6\\.0.*");
//    gzipHandler.setIncludedMethods("GET");
//
     server.setHandler(chain);
    // ShutdownThread.deregister(server);
  }


  /** descendants may inject own handler chaining it to the given root
   * and then returning that own one*/
  protected HandlerWrapper injectJettyHandlers(HandlerWrapper chain) {
    return chain;
  }

  @Override
  public String toString() {
    return "JettySolrRunner: " + jettyPort;
  }

  /**
   * @return the {@link SolrDispatchFilter} for this node
   */
  public SolrDispatchFilter getSolrDispatchFilter() { return dispatchFilter == null ? null : (SolrDispatchFilter) dispatchFilter.getFilter(); }

  /**
   * @return the {@link CoreContainer} for this node
   */
  public CoreContainer getCoreContainer() {
    if (getSolrDispatchFilter() == null || getSolrDispatchFilter().getCores() == null) {
      return null;
    }
    return getSolrDispatchFilter().getCores();
  }

  public String getNodeName() {
    return nodeName;
  }

  public boolean isRunning() {
    return server.isRunning();
  }

  public boolean isStopped() {
    return (server.isStopped() && dispatchFilter == null) || (server.isStopped() && dispatchFilter.isStopped());
  }

  // ------------------------------------------------------------------------------------------------
  // ------------------------------------------------------------------------------------------------

  /**
   * Start the Jetty server
   *
   * If the server has been started before, it will restart using the same port
   *
   * @throws Exception if an error occurs on startup
   */
  public void start() throws Exception {
    start(true);
  }

  /**
   * Start the Jetty server
   *
   * @param reusePort when true, will start up on the same port as used by any
   *                  previous runs of this JettySolrRunner.  If false, will use
   *                  the port specified by the server's JettyConfig.
   *
   * @throws Exception if an error occurs on startup
   */
  public void start(boolean reusePort) throws Exception {
    closed = false;
    // Do not let Jetty/Solr pollute the MDC for this thread
    Map<String, String> prevContext = MDC.getCopyOfContextMap();
    MDC.clear();

    try {
      int port = reusePort && jettyPort != -1 ? jettyPort : this.config.port;
      log.info("Start Jetty (configured port={}, binding port={})", this.config.port, port);
      if (startedBefore) {
        connector.setPort(port);
      }

      // if !started before, make a new server

      if (!server.isRunning()) {
        if (config.portRetryTime > 0) {
          retryOnPortBindFailure(port);
        } else {
          server.start();
        }
      }

      if (getCoreContainer() != null && getCoreContainer().isZooKeeperAware()) {
        nodeName = getCoreContainer().getZkController().getNodeName();
        if (nodeName == null) throw new IllegalStateException();
      }

      setProtocolAndHost();

      if (enableProxy) {
        if (started) {
          proxy.reopen();
        } else {
          proxy.open(new URI(getBaseUrl()));
        }
      }

    } finally {
      started  = true;

      if (prevContext != null)  {
        MDC.setContextMap(prevContext);
      } else {
        MDC.clear();
      }
    }
  }

  private void retryOnPortBindFailure(int port) throws Exception {
    int tryCnt = 1;
    while (tryCnt < 3) {
      try {
        if (log.isDebugEnabled()) log.debug(" {} try number {} ...", port, tryCnt);
        server.start();
        break;
      } catch (IOException ioe) {
        Exception e = lookForBindException(ioe);
        if (e instanceof BindException) {
          log.info("Port is in use, will try again");
          server.stop();

          tryCnt++;
          continue;
        }

        throw e;
      }
    }
  }

  private void setProtocolAndHost() {
    String protocol;

    Connector[] conns = server.getConnectors();
    if (0 == conns.length) {
      throw new IllegalStateException("Jetty Server has no Connectors");
    }
    ServerConnector c = (ServerConnector) conns[0];

    protocol = c.getDefaultProtocol().toLowerCase(Locale.ROOT).startsWith("ssl") ? "https" : "http";

    this.protocol = protocol;
    this.host = c.getHost();
  }

  /**
   * Traverses the cause chain looking for a BindException. Returns either a bind exception
   * that was found in the chain or the original argument.
   *
   * @param ioe An IOException that might wrap a BindException
   * @return A bind exception if present otherwise ioe
   */
  static Exception lookForBindException(IOException ioe) {
    Exception e = ioe;
    while(true) {
      final Throwable eCause = e.getCause();
      if (eCause == null) break;
      Throwable cause = eCause;
      if (!(e != cause && !(e instanceof BindException))) break;
      if (eCause instanceof Exception) {
        e = (Exception) eCause;
        if (e instanceof BindException) {
          return e;
        }
      }
    }
    return ioe;
  }

  @Override
  public void close() throws IOException {
    if (closed) return;
    closed = true;

    // Do not let Jetty/Solr pollute the MDC for this thread
    Map<String,String> prevContext = MDC.getCopyOfContextMap();
    MDC.clear();
    try {

      try {

        server.stop();
        //((LifeCycle)server.getThreadPool()).stop();

      } catch (Exception e) {
        log.error("Error stopping jetty server", e);
      }

//      if (config.qtp == null) {
//        IOUtils.closeQuietly(qtp);
//      }

      try {
        server.join();
      } catch (InterruptedException e) {

      }

    } catch (Exception e) {
      log.error("Exception stopping jetty", e);
      throw new RuntimeException(e);
    } finally {

      if (enableProxy) {
        proxy.close();
      }

      if (reporter != null) {
        reporter.close();
      }

      assert ObjectReleaseTracker.getInstance().release(this);
      if (prevContext != null) {
        MDC.setContextMap(prevContext);
      } else {
        MDC.clear();
      }
    }
  }

  /**
   * Stop the Jetty server
   *
   * @throws Exception if an error occurs on shutdown
   */
  public void stop() throws Exception {
    close();
  }

  /**
   * Returns the Local Port of the jetty Server.
   *
   * @exception RuntimeException if there is no Connector
   */
  private int getFirstConnectorPort() {
    Connector[] conns = server.getConnectors();
    if (0 == conns.length) {
      throw new RuntimeException("Jetty Server has no Connectors");
    }
    return ((ServerConnector) conns[0]).getLocalPort();
  }


  /**
   * Returns the Local Port of the jetty Server.
   *
   * @exception RuntimeException if there is no Connector
   */
  public int getLocalPort() {
    return getLocalPort(false);
  }

  /**
   * Returns the Local Port of the jetty Server.
   *
   * @param internalPort pass true to get the true jetty port rather than the proxy port if configured
   *
   * @exception RuntimeException if there is no Connector
   */
  public int getLocalPort(boolean internalPort) {
    if (jettyPort == -1) {
      throw new IllegalStateException("You cannot get the port until this instance has started");
    }
    if (internalPort ) {
      return jettyPort;
    }
    return (proxyPort != -1) ? proxyPort : jettyPort;
  }

  /**
   * Sets the port of a local socket proxy that sits infront of this server; if set
   * then all client traffic will flow through the proxy, giving us the ability to
   * simulate network partitions very easily.
   */
  public void setProxyPort(int proxyPort) {
    this.proxyPort = proxyPort;
  }

  /**
   * Returns a base URL consisting of the protocol, host, and port for a
   * Connector in use by the Jetty Server contained in this runner.
   */
  public String getBaseUrl() {
      return protocol +"://" + host + ":" + jettyPort + config.context;
  }

  public String getBaseURLV2(){
    return protocol +"://" + host + ":" + jettyPort + "/api";
  }
  /**
   * Returns a base URL consisting of the protocol, host, and port for a
   * Connector in use by the Jetty Server contained in this runner.
   */
  public String getProxyBaseUrl() {
    return protocol +"://" + host + ":" + getLocalPort() + config.context;
  }

  public SolrClient newClient() {
    return new Http2SolrClient.Builder(getBaseUrl()).
            withHttpClient(getCoreContainer().getUpdateShardHandler().getTheSharedHttpClient()).build();
  }

  public SolrClient newClient(String collection) {
    return new Http2SolrClient.Builder(getBaseUrl() + "/" + collection).
        withHttpClient(getCoreContainer().getUpdateShardHandler().getTheSharedHttpClient()).build();
  }


  public SolrClient newHttp2Client() {
    return new Http2SolrClient.Builder(getBaseUrl()).
        withHttpClient(getCoreContainer().getUpdateShardHandler().getTheSharedHttpClient()).build();
  }

  public SolrClient newClient(int connectionTimeoutMillis, int socketTimeoutMillis) {
    return new Http2SolrClient.Builder(getBaseUrl())
        .connectionTimeout(connectionTimeoutMillis)
        .idleTimeout(socketTimeoutMillis)
        .withHttpClient(getCoreContainer().getUpdateShardHandler().getTheSharedHttpClient())
        .build();
  }

  public DebugFilter getDebugFilter() {
    return (DebugFilter)debugFilter.getFilter();
  }

  // --------------------------------------------------------------
  // --------------------------------------------------------------

  /**
   * This is a stupid hack to give jetty something to attach to
   */
  public static class Servlet404 extends HttpServlet {
    @Override
    public void service(HttpServletRequest req, HttpServletResponse res)
        throws IOException {
      res.setStatus(404);
      //res.sendError(404, "Can not find (404 from jetty Solr Runner): " + req.getRequestURI());
    }
  }

  /**
   * A main class that starts jetty+solr This is useful for debugging
   */
  public static void main(String[] args) throws Exception {
    JettySolrRunner jetty = new JettySolrRunner(".", "/solr", 8983);
    jetty.start();
  }

  /**
   * @return the Solr home directory of this JettySolrRunner
   */
  public String getSolrHome() {
    return solrHome;
  }

  public String getHost() {
    return host;
  }

  /**
   * @return this node's properties
   */
  public Properties getNodeProperties() {
    return nodeProperties;
  }

  static class Delay {
    final AtomicInteger counter;
    final int delayValue;
    final String reason;

    public Delay(String reason, int counter, int delay) {
      this.reason = reason;
      this.counter = new AtomicInteger(counter);
      this.delayValue = delay;
    }
  }

  public SocketProxy getProxy() {
    return proxy;
  }

}
