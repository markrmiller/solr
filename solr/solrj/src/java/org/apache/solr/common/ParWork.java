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
package org.apache.solr.common;

import java.io.Closeable;
import java.lang.invoke.MethodHandles;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.OrderedExecutor;
import org.apache.solr.common.util.SysStats;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ParWork. A workhorse utility class that tries to use good patterns,
 * parallelism
 * 
 */
public class ParWork implements Closeable {
  private final static boolean TRACK_TIMES = false;

  public static final int PROC_COUNT = ManagementFactory.getOperatingSystemMXBean().getAvailableProcessors();
  public static final String ROOT_EXEC_NAME = "ROOT_SHARED:";
  private static final String WORK_WAS_INTERRUPTED = "Work was interrupted!";

  private static final String RAN_INTO_AN_ERROR_WHILE_DOING_WORK =
      "Ran into an error while doing work!";

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected final static ThreadLocal<ExecutorService> THREAD_LOCAL_EXECUTOR = new ThreadLocal<>();

  private final String rootLabel;

  private Queue<Object> collectSet = new LinkedList<>();

  private static volatile ParWorkExecutor EXEC;
  private static final ReentrantLock EXEC_LOCK = new ReentrantLock(false);

  public static ParWorkExecutor getRootSharedExecutor() {
    if (EXEC == null) {
      EXEC_LOCK.lock();
      try {
        if (EXEC == null) {
          Integer coreSize = Integer.getInteger("solr.rootSharedThreadPoolCoreSize", 512);
          EXEC = (ParWorkExecutor) getParExecutorService(ROOT_EXEC_NAME, coreSize
             , Integer.MAX_VALUE, 500000,
              new SynchronousQueue());
          EXEC.enableCloseLock();

          EXEC.prestartAllCoreThreads();
        }
      } finally {
        EXEC_LOCK.unlock();
      }
    }
    return EXEC;
  }

  public static Future<?> submit(String threadName, Runnable task) {
    if (task == null) throw new NullPointerException();
    RunnableFuture<Void> ftask = EXEC.newTaskFor(threadName, task, null);
    EXEC.execute(ftask);
    return ftask;
  }


  public static <T> Future<T> submit(String threadName, Runnable task, T result) {
    if (task == null) throw new NullPointerException();
    RunnableFuture<T> ftask = EXEC.newTaskFor(threadName, task, result);
    EXEC.execute(ftask);
    return ftask;
  }


  public static <T> Future<T> submit(String threadName, Callable<T> task) {
    if (task == null) throw new NullPointerException();
    RunnableFuture<T> ftask = EXEC.newTaskFor(threadName, task);
    EXEC.execute(ftask);
    return ftask;
  }

  public static void shutdownParWorkExecutor() {
    shutdownParWorkExecutor(true);
  }

  public static void shutdownParWorkExecutor(boolean wait) {
    EXEC_LOCK.lock();
    try {
      try {
        shutdownParWorkExecutor(EXEC, wait);
      } finally {
        EXEC = null;
      }
    } finally {
      EXEC_LOCK.unlock();
    }
  }

  public static void shutdownParWorkExecutor(ParWorkExecutor executor, boolean wait) {
    if (executor != null) {
      executor.disableCloseLock();
      executor.shutdownNow();
      if (wait) {
        ExecutorUtil.awaitTermination(executor);
      }
    }
  }


  private static final SysStats sysStats = SysStats.getSysStats();

  public static SysStats getSysStats() {
    return sysStats;
  }

    private static class WorkUnit {
    private final Collection<Object> objects;
    private final TimeTracker tracker;

    public WorkUnit(Collection<Object> objects, TimeTracker tracker) {
      this.objects = objects;
      this.tracker = tracker;
    }
  }

  private static final Set<Class> OK_CLASSES;

  static {
    OK_CLASSES = Set.of(ExecutorService.class, OrderedExecutor.class, Closeable.class, AutoCloseable.class, Callable.class, Runnable.class, Timer.class, CloseableHttpClient.class, Map.class);
  }

  private final Queue<WorkUnit> workUnits = new LinkedList<>();

  private volatile TimeTracker tracker;

  private final boolean ignoreExceptions;

  private final Set<Throwable> warns = ConcurrentHashMap.newKeySet(6);

  // TODO should take logger as well
  public static class Exp extends Exception {

    private static final String ERROR_MSG = "Solr ran into an unexpected Exception";

    /**
     * Handles exceptions correctly for you, including logging.
     * 
     * @param msg message to include to clarify the problem
     */
    public Exp(String msg) {
      this(null, msg, null);
    }

    /**
     * Handles exceptions correctly for you, including logging.
     * 
     * @param th the exception to handle
     */
    public Exp(Throwable th) {
      this(null, th.getMessage(), th);
    }

    /**
     * Handles exceptions correctly for you, including logging.
     * 
     * @param msg message to include to clarify the problem
     * @param th  the exception to handle
     */
    public Exp(String msg, Throwable th) {
      this(null, msg, th);
    }

    public Exp(Logger classLog, String msg, Throwable th) {
      super(msg == null ? ERROR_MSG : msg, th);

      Logger logger;
      if (classLog != null) {
        logger = classLog;
      } else {
        logger = log;
      }

      logger.error(ERROR_MSG, th);
      if (th != null && th instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      if (th != null && th instanceof KeeperException) { // TODO maybe start using ZooKeeperException
        if (((KeeperException) th).code() == KeeperException.Code.SESSIONEXPIRED) {
          log.warn("The session has expired, give up any leadership roles!");
        }
      }
    }
  }

  public ParWork(Object object) {
    this(object, false);
  }

  public ParWork(Object object, boolean ignoreExceptions) {
    this.ignoreExceptions = ignoreExceptions;
    this.rootLabel = object instanceof String ?
        (String) object : object.getClass().getSimpleName();
    if (TRACK_TIMES) tracker = new TimeTracker(object, object == null ? "NullObject" : object.getClass().getName());
    // constructor must stay very light weight
  }

  public void collect(String label, Object object) {
    if (object == null) {
      return;
    }
    if (collectSet == null) {
      collectSet = new LinkedList<>();
    }
    gatherObjects(label, object, collectSet);
  }

  public void collect(Object object) {
   collect(object != null ? object instanceof String ? (String) object : object.getClass().getSimpleName() : null, object);
  }

  public void collect(Object... objects) {
    for (Object object : objects) {
      collect(object);
    }
  }

  /**
   * @param callable A Callable to run. If an object is return, it's toString is
   *                 used to identify it.
   */
  public void collect(String label, Callable<?> callable) {
    collect(label, (Object) callable);
  }

  /**
   * @param runnable A Runnable to run. If an object is return, it's toString is
   *                 used to identify it.
   */
  public void collect(String label, Runnable runnable) {
    collect(label, (Object) runnable);
  }

  public void addCollect() {
    if (collectSet == null || collectSet.isEmpty()) {
      //if (log.isDebugEnabled()) log.debug("No work collected to submit", new RuntimeException());
      return;
    }
    try {
      add(collectSet);
    } finally {
      collectSet = null;
    }
  }

  private void gatherObjects(String label, Object submittedObject, Collection<Object> collectSet) {
    if (submittedObject != null) {
      if (submittedObject instanceof Collection) {
        for (Object obj : (Collection) submittedObject) {
          verifyValidType(obj);
          collectSet.add(obj);
        }
      } else if (submittedObject instanceof Map) {
        ((Map) submittedObject).forEach((k, v) -> {
          verifyValidType(v);
          collectSet.add(v);
        });
      } else {
        verifyValidType(submittedObject);
        collectSet.add(submittedObject);
      }
    }
  }

  private void add(Collection<Object> objects) {
    WorkUnit workUnit = new WorkUnit(objects, tracker);
    workUnits.add(workUnit);
  }

  private void verifyValidType(Object object) {
    boolean ok = false;
    for (Class okobject : OK_CLASSES) {
      if (okobject.isAssignableFrom(object.getClass())) {
        ok = true;
        break;
      }
    }
    if (!ok) {
      log.error(" -> I do not know how to close: " + object.getClass().getName());
      throw new IllegalArgumentException(" -> I do not know how to close: " + object.getClass().getName());
    }
  }

  @Override
  public void close() {

    addCollect();

    boolean needExec = false;
    for (WorkUnit workUnit : workUnits) {
      if (workUnit.objects.size() > 1) {
        needExec = true;
        break;
      }
    }

    PerThreadExecService executor = null;
    if (needExec) {
      executor = (PerThreadExecService) getMyPerThreadExecutor();
    }
    //initExecutor();
    AtomicReference<Throwable> exception = new AtomicReference<>();
    try {
      for (WorkUnit workUnit : workUnits) {
        if (log.isTraceEnabled()) log.trace("Process workunit {} {}", rootLabel, workUnit.objects);
        TimeTracker workUnitTracker = null;
        if (TRACK_TIMES) workUnitTracker = workUnit.tracker.startSubClose(workUnit);
        try {
          Collection<Object> objects = workUnit.objects;

          if (objects.size() == 1) {
            handleObject(exception, workUnitTracker, objects.iterator().next());
          } else {

            List<Callable<Object>> closeCalls = new ArrayList<>(objects.size());

            for (Object object : objects) {
              if (object == null) continue;
              closeCalls.add(() -> {
                handleObject(exception, workUnitTracker, object);
                return object;
              });
            }
            if (closeCalls.size() > 0) {

              List<Future<Object>> results = new ArrayList<>(closeCalls.size());

              for (Callable call : closeCalls) {
                Future future = executor.submit(call);
                results.add(future);
              }

              int i = 0;
              for (Future<Object> future : results) {
                  try {
                    future.get(Long.getLong("solr.parwork.task_timeout", TimeUnit.MINUTES.toMillis(10)), TimeUnit.MILLISECONDS); // MRM TODO: timeout
                  } catch (Error error) {
                    log.error("Error in ParWork", error);
                    throw error;
                  } catch (Throwable t) {
                    exception.updateAndGet(throwable -> {
                      if (throwable == null) {
                        return t;
                      } else {
                        throwable.addSuppressed(t);
                        return throwable;
                      }
                    });
                  }
                  if (!future.isDone() || future.isCancelled()) {
                    log.warn("A task did not finish isDone={} isCanceled={}", future.isDone(), future.isCancelled());
                  }
              }
            }
          }
        } finally {
          if (workUnitTracker != null)
            workUnitTracker.doneClose();
        }

      }
    } catch (Throwable t) {
      log.error(RAN_INTO_AN_ERROR_WHILE_DOING_WORK, t);

      if (exception.get() == null) {
        exception.set(t);
      }
    } finally {

      if (TRACK_TIMES) tracker.doneClose();
      
      //System.out.println("DONE:" + tracker.getElapsedMS());

      // warns.forEach((it) -> log.warn(RAN_INTO_AN_ERROR_WHILE_DOING_WORK, new RuntimeException(it)));

      if (exception.get() != null) {
        Throwable exp = exception.get();
        if (exp instanceof RuntimeException) {
          throw (RuntimeException) exp;
        }
        ParWorkException rte = new ParWorkException(exp);
        rte.fillInStackTrace();
        throw rte;
      }
    }
  }

  public static class ParWorkException extends RuntimeException {

    public ParWorkException(Throwable exp) {
    }
  }

  public static ExecutorService getMyPerThreadExecutor() {
    //Thread thread = Thread.currentThread();

    ExecutorService service = null;
//    if (thread instanceof  SolrThread) {
//      service = ((SolrThread) thread).getExecutorService();
//    }

    if (service == null) {
      ExecutorService exec = THREAD_LOCAL_EXECUTOR.get();
      if (exec == null) {
        if (log.isTraceEnabled()) {
          log.trace("Starting a new executor");
        }

        Integer minThreads;
        Integer maxThreads;
        minThreads = 4;
        maxThreads = Integer.getInteger("solr.perThreadPoolSize", PROC_COUNT);
        exec = getExecutorService("ThreadLocalExec", Math.max(minThreads, maxThreads)); // keep alive directly affects how long a worker might
        // be stuck in poll without an enqueue on shutdown
        THREAD_LOCAL_EXECUTOR.set(exec);
      }
      service = exec;
    }

    return service;
  }

  public static ExecutorService getParExecutorService(String name, int corePoolSize, int maxPoolSize, int keepAliveTime, BlockingQueue queue) {
    ThreadPoolExecutor exec;
    exec = new ParWorkExecutor(name,
            corePoolSize, maxPoolSize, keepAliveTime, queue);
    return exec;
  }

  public static ExecutorService getExecutorService(String name, int maximumPoolSize) {
    return new PerThreadExecService(name, getRootSharedExecutor(), maximumPoolSize);
  }

  private void handleObject(AtomicReference<Throwable> exception, final TimeTracker workUnitTracker, Object ob) {
    if (log.isTraceEnabled()) log.trace(
          "handleObject(AtomicReference<Throwable> exception={}, CloseTimeTracker workUnitTracker={}, Object object={}) - start",
          exception, workUnitTracker, ob);

    Object object = ob;
    Object returnObject = null;
    TimeTracker subTracker = null;
    if (TRACK_TIMES) subTracker = workUnitTracker.startSubClose(object);
    try {
      boolean handled = false;
      if (object instanceof OrderedExecutor) {
        ((OrderedExecutor) object).shutdownAndAwaitTermination();
        handled = true;
      } else if (object instanceof ExecutorService) {
        shutdownAndAwaitTermination((ExecutorService) object);
        handled = true;
      } else if (object instanceof CloseableHttpClient) {
        HttpClientUtil.close((CloseableHttpClient) object);
        handled = true;
      } else if (object instanceof Closeable) {
        ((Closeable) object).close();
        handled = true;
      } else if (object instanceof AutoCloseable) {
        ((AutoCloseable) object).close();
        handled = true;
      } else if (object instanceof Callable) {
        returnObject = ((Callable<?>) object).call();
        handled = true;
      } else if (object instanceof Runnable) {
        ((Runnable) object).run();
        handled = true;
      } else if (object instanceof Timer) {
        ((Timer) object).cancel();
        handled = true;
      }

      if (!handled) {
        String msg = " -> I do not know how to close " + object.getClass().getName();
        log.error(msg);
        exception.updateAndGet(throwable -> {
          if (throwable == null) {
            return new IllegalArgumentException(msg);
          } else {
            throwable.addSuppressed(new IllegalArgumentException(msg));
            return throwable;
          }
        });
      }
    } catch (Throwable t) {
      if (ignoreExceptions) {
        warns.add(t);
        log.error("Error handling close for an object: " + object.getClass().getSimpleName(), new ObjectReleaseTracker.ObjectTrackerException(t));
        if (t instanceof Error && !(t instanceof AssertionError)) {
          throw (Error) t;
        }
      } else {
        log.error("handleObject(AtomicReference<Throwable>=" + exception + ", CloseTimeTracker=" + workUnitTracker + ")" + ", Object=" + object + ")", t);
        propagateInterrupt(t);
        if (t instanceof Error) {
          throw (Error) t;
        }
        if (t instanceof RuntimeException) {
          throw (RuntimeException) t;
        } else {
          throw new WorkException(RAN_INTO_AN_ERROR_WHILE_DOING_WORK, t); // TODO, hmm how do I keep zk session timeout and interrupt in play?
        }
      }

    } finally {
      if (TRACK_TIMES) subTracker.doneClose(returnObject instanceof String ? (String) returnObject : (returnObject == null ? "" : returnObject.getClass().getName()));
    }

    if (log.isTraceEnabled()) log.trace("handleObject(AtomicReference<Throwable>, CloseTimeTracker, List<Callable<Object>>, Object) - end");
  }

  public static <K> Set<K> concSetSmallO() {
    return ConcurrentHashMap.newKeySet(50);
  }

  public static <K, V> ConcurrentHashMap<K, V> concMapSmallO() {
    return new ConcurrentHashMap<K, V>(132, 0.75f, 50);
  }

  public static <K, V> ConcurrentHashMap<K, V> concMapReqsO() {
    return new ConcurrentHashMap<>(128, 0.75f, 2048);
  }

  public static <K, V> ConcurrentHashMap<K, V> concMapClassesO() {
    return new ConcurrentHashMap<>(132, 0.75f, 8192);
  }

  public static void propagateInterrupt(Throwable t) {
    propagateInterrupt(t, false);
  }

  public static void propagateInterrupt(Throwable t, boolean infoLogMsg) {
    if (t instanceof InterruptedException) {
      log.warn("Interrupted {} while doing work", t.getMessage(), t);
      Thread.currentThread().interrupt();
    } else {
      if (infoLogMsg) {
        if (log.isDebugEnabled()) {
          log.info(t.getClass().getName() + " " + t.getMessage(), t);
        } else {
          log.info(t.getClass().getName() + " " + t.getMessage(), t);
        }
      } else {
        log.error("Solr ran into an exception", t);
      }
    }

    if (t instanceof Error) {
      throw (Error) t;
    }
  }

  public static void propagateInterrupt(String msg, Throwable t) {
    propagateInterrupt(msg, t, false);
  }

  public static void propagateInterrupt(String msg, Throwable t, boolean infoLogMsg) {
    if (t != null && t instanceof InterruptedException) {
      log.info("Interrupted", t);
      Thread.currentThread().interrupt();
    } else {
      if (infoLogMsg) {
        log.info(msg);
      } else {
        log.warn(msg, t);
      }
    }
    if (t != null && t instanceof Error) {
      throw (Error) t;
    }
  }

  public static void shutdownAndAwaitTermination(ExecutorService pool) {
    if (pool == null)
      return;
    pool.shutdown(); // Disable new tasks from being submitted
    awaitTermination(pool);
    if (!(pool.isShutdown())) {
      throw new RuntimeException("Timeout waiting for executor to shutdown");
    }

  }

  public static void awaitTermination(ExecutorService pool) {
    boolean shutdown = false;
    while (!shutdown) {
      try {
        // Wait a while for existing tasks to terminate
        shutdown = pool.awaitTermination(30, TimeUnit.SECONDS);
      } catch (InterruptedException ie) {
        // Preserve interrupt status
        Thread.currentThread().interrupt();
      }
    }
  }

  public static abstract class ParWorkCallableBase<V> implements Callable<Object> {
    @Override
    public abstract Object call() throws Exception;

    public abstract boolean isCallerThreadAllowed();
  }
}
