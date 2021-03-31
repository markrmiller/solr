package org.apache.solr.client.solrj.impl;

import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.Phaser;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

public class AsyncTracker implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final long CLOSE_TIMEOUT = TimeUnit.SECONDS.convert(1, TimeUnit.HOURS);

  private final Semaphore available;

  private volatile boolean closed = false;

  private final ReentrantLock waitForCompleteLock = new ReentrantLock(false);

  // wait for async requests
  private final Phaser phaser = new ThePhaser(1);
  // maximum outstanding requests left

  public static class ThePhaser extends Phaser {

    ThePhaser(int start) {
      super(start);
    }

    @Override
    protected boolean onAdvance(int phase, int parties) {
      return false;
    }
  }

  public AsyncTracker(int maxOutstandingAsyncRequests) {
    if (maxOutstandingAsyncRequests > 0) {
      available = new Semaphore(maxOutstandingAsyncRequests, false);
    } else {
      available = null;
    }
  }

  public void waitForComplete(long timeout, TimeUnit timeUnit) {
    waitForCompleteLock.lock();
    try {
      if (phaser.getRegisteredParties() == 1) {
        return;
      }
      if (log.isTraceEnabled())
        log.trace("Before wait for outstanding requests registered: {} arrived: {}, {} {}", phaser.getRegisteredParties(), phaser.getArrivedParties(), phaser.getUnarrivedParties(), phaser);
      try {
        phaser.awaitAdvanceInterruptibly(phaser.arrive(), timeout, timeUnit);
      } catch (IllegalStateException e) {
        log.error("Unexpected, perhaps came after close; ?", e);
      } catch (InterruptedException e) {
        ParWork.propagateInterrupt(e);
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      } catch (TimeoutException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Timeout waiting for outstanding async requests", e);
      }

      if (log.isTraceEnabled()) log.trace("After wait for outstanding requests {}", phaser);
    } finally {
      waitForCompleteLock.unlock();
    }
  }

  public void close() {
    try {
      if (available != null) {
        while (true) {
          final boolean hasQueuedThreads = available.hasQueuedThreads();
          if (!hasQueuedThreads) break;
          available.release(available.getQueueLength());
        }
      }
      phaser.forceTermination();
    } catch (Exception e) {
      log.error("Exception closing Http2SolrClient asyncTracker", e);
    } finally {
      closed = true;
    }
  }

  public void register() {
    if (log.isDebugEnabled()) {
      log.debug("Registered new party {}", phaser);
    }
    try {
      if (available != null) {
        available.acquire();
      }
    } catch (InterruptedException e) {
      ParWork.propagateInterrupt(e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
    phaser.register();

  }

  public void arrive() {
    try {
      try {
        phaser.arriveAndDeregister();
      } catch (IllegalStateException e) {
        if (closed) {
          log.warn("Came after close", e);
        } else {
          throw e;
        }
      }
    } finally {
      if (available != null) available.release();
    }
    if (log.isDebugEnabled()) log.debug("Request complete {}", phaser);
  }

  public int getUnArrived() {
    return phaser.getUnarrivedParties();
  }
}
