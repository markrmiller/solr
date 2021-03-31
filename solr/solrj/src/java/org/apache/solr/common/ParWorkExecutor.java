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

import org.apache.solr.common.util.CloseTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

public class ParWorkExecutor extends ThreadPoolExecutor {
  private static final Logger log = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private static class ParWorkFutureTask<T> extends FutureTask {

    final String threadName;
    private String oldThreadName;

    public ParWorkFutureTask(String threadName, Callable callable) {
      super(callable);
      this.threadName = threadName;
    }

    public ParWorkFutureTask(String threadName, Runnable runnable, Object value) {
      super(runnable, value);
      this.threadName = threadName;
    }

    public void updateThreadName() {
      if (oldThreadName == null) {
        this.oldThreadName = Thread.currentThread().getName();
        Thread.currentThread().setName(threadName);
      } else {
        Thread.currentThread().setName(oldThreadName);
        oldThreadName = null;
      }
    }
  }

  public static final int KEEP_ALIVE_TIME = 10000;


  private CloseTracker closeTracker;

  public ParWorkExecutor(String name, int maxPoolsSize) {
    this(name, 4, maxPoolsSize, KEEP_ALIVE_TIME, new LinkedBlockingDeque<>());
  }

  public ParWorkExecutor(String name, int corePoolsSize, int maxPoolsSize,
      int keepalive, BlockingQueue<Runnable> workQueue) {
    super(corePoolsSize, Math.max(corePoolsSize, maxPoolsSize), keepalive, TimeUnit.MILLISECONDS, workQueue, new ThreadFactory() {
      AtomicInteger threadNumber = new AtomicInteger(1);
      ThreadGroup group;

      {
        SecurityManager s = System.getSecurityManager();
        group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
      }

      @Override
      public Thread newThread(Runnable r) {
        Thread t = new Thread(group, r, "ParWork" + threadNumber.getAndIncrement(), 0);
        t.setDaemon(false);
        return t;
      }
    });
    assert (closeTracker = new CloseTracker(false)) != null;
  }

  public void shutdown() {
//    if (isShutdown()) {
//      return;
//    }
    assert closeTracker != null ? closeTracker.close() : true;
  //  setKeepAliveTime(1, TimeUnit.NANOSECONDS);
//    for (int i = 0; i < Math.max(0, getPoolSize() - getActiveCount() + 1); i++) {
//      try {
//        submit(() -> {
//        });
//      } catch (RejectedExecutionException e) {
//        break;
//      }
//    }
//    setKeepAliveTime(1, TimeUnit.NANOSECONDS);
//    allowCoreThreadTimeOut(true);

    super.shutdown();
  }

  public List<Runnable> shutdownNow() {
    return super.shutdownNow();
  }

  public void enableCloseLock() {
    if (this.closeTracker != null) {
      this.closeTracker.enableCloseLock();
    }
  }

  public void disableCloseLock() {
    if (this.closeTracker != null) {
      this.closeTracker.disableCloseLock();
    }
  }

  protected <T> RunnableFuture<T> newTaskFor(String threadName, Runnable runnable, T value) {
    return new ParWorkFutureTask<T>(threadName, runnable, value);
  }

  protected <T> RunnableFuture<T> newTaskFor(String threadName, Callable<T> callable) {
    return new ParWorkFutureTask<T>(threadName, callable);
  }

  @Override
  protected void beforeExecute(Thread t, Runnable r) {
    if (r instanceof ParWorkFutureTask) {
      ((ParWorkFutureTask) r).updateThreadName();
    }
  }

  @Override
  protected void afterExecute(Runnable r, Throwable t) {
    if (r instanceof ParWorkFutureTask) {
      ((ParWorkFutureTask) r).updateThreadName();
    }
  }

  @Override
  public void execute(Runnable command) {
    try {
      super.execute(command);
    } catch (Throwable t) {
      log.error("ParWorkExecutor exception in execute", t);
      throw t;
    }
  }
}
