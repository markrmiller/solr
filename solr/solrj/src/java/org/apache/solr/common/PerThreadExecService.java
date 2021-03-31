package org.apache.solr.common;

import org.apache.solr.client.solrj.impl.AsyncTracker;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;

public class PerThreadExecService extends AbstractExecutorService {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final int MAX_AVAILABLE = Math.max(ParWork.PROC_COUNT / 2, 3);

  private final ParWorkExecutor service;
  private final int maxSize;
  private final String name;
  private volatile boolean terminated;
  private volatile boolean shutdown;

  private final AsyncTracker asyncTracker;

  public PerThreadExecService(String name, ParWorkExecutor service, int maxSize) {
    assert service != null;
    this.name = name;
    if (maxSize == -1) {
      this.maxSize = MAX_AVAILABLE;
    } else {
      this.maxSize = maxSize;
    }
    this.service = service;
    int maxOutstandingAsyncRequests = MAX_AVAILABLE;
    asyncTracker = new AsyncTracker(maxOutstandingAsyncRequests);
  }

  @Override
  public void shutdown() {
    assert ObjectReleaseTracker.release(this);
    this.shutdown = true;
    IOUtils.closeQuietly(asyncTracker);
  }

  @Override
  public List<Runnable> shutdownNow() {
    shutdown = true;
    asyncTracker.close();
    return Collections.emptyList();
  }

  @Override
  public boolean isShutdown() {
    return shutdown;
  }

  @Override
  public boolean isTerminated() {
    return terminated;
  }

  @Override
  public boolean awaitTermination(long l, TimeUnit timeUnit) throws InterruptedException {

    asyncTracker.waitForComplete(l, timeUnit);

    return true;
  }

  @Override
  public void execute(Runnable runnable) {
    try {
      if (asyncTracker.getUnArrived() > maxSize) {
        runnable.run();
      } else {
        service.submit(runnable);
      }
    } catch (Throwable t) {
      log.error(Class.class.getSimpleName() + " Exception", t);
      throw t;
    }
  }
}
