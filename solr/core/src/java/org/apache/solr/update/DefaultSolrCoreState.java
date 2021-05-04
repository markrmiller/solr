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
package org.apache.solr.update;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.search.Sort;
import org.apache.lucene.store.Directory;
import org.apache.solr.cloud.ActionThrottle;
import org.apache.solr.cloud.RecoveryStrategy;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.SolrCore;
import org.apache.solr.index.SortingMergePolicy;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.util.RefCounted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public final class DefaultSolrCoreState extends SolrCoreState implements RecoveryStrategy.RecoveryListener {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final AtomicReference<Boolean> needsRecovery = new AtomicReference<>();

  private volatile boolean recoveryRunning = false;

  private final boolean SKIP_AUTO_RECOVERY = Boolean.getBoolean("solrcloud.skip.autorecovery");


  private final ActionThrottle recoveryThrottle = new ActionThrottle("recovery", Integer.getInteger("solr.recoveryThrottle", 0));

  private final AtomicInteger recoveryWaiting = new AtomicInteger();

  // Use the readLock to retrieve the current IndexWriter (may be lazily opened)
  // Use the writeLock for changing index writers
  private final ReentrantReadWriteLock iwLock = new ReentrantReadWriteLock(true);
  private final ReentrantLock iwOpenLock = new ReentrantLock(true);

  private volatile SolrIndexWriter indexWriter = null;
  private final DirectoryFactory directoryFactory;
  private final RecoveryStrategy.Builder recoveryStrategyBuilder;

  private volatile RecoveryStrategy recoveryStrat;


  private volatile boolean lastReplicationSuccess = true;

  // will we attempt recovery as if we just started up (i.e. use starting versions rather than recent versions for peersync
  // so we aren't looking at update versions that have started buffering since we came up.
  private volatile boolean recoveringAfterStartup = true;

  private volatile RefCounted<IndexWriter> refCntWriter;

  protected final ReentrantLock commitLock = new ReentrantLock();

  private final AtomicBoolean cdcrRunning = new AtomicBoolean();

  private volatile Future<Boolean> cdcrBootstrapFuture;

  private volatile Callable cdcrBootstrapCallable;

  private volatile boolean prepForClose;


  @Deprecated
  public DefaultSolrCoreState(DirectoryFactory directoryFactory) {
    this(directoryFactory, new RecoveryStrategy.Builder());
  }

  public DefaultSolrCoreState(DirectoryFactory directoryFactory,
                              RecoveryStrategy.Builder recoveryStrategyBuilder) {
    this.directoryFactory = directoryFactory;
    this.recoveryStrategyBuilder = recoveryStrategyBuilder;
  }

  private void closeIndexWriter(IndexWriterCloser closer) {
    try {
      if (log.isDebugEnabled()) log.debug("SolrCoreState ref count has reached 0 - closing IndexWriter");
      if (closer != null) {
        if (log.isDebugEnabled()) log.debug("closing IndexWriter with IndexWriterCloser");

        // indexWriter may be null if there was a failure in opening the search during core init,
        // such as from an index corruption issue (see TestCoreAdmin#testReloadCoreAfterFailure)
        if (indexWriter != null) {
          closer.closeWriter(indexWriter);
        }
      } else if (indexWriter != null) {
        log.debug("closing IndexWriter...");
        indexWriter.commit();
        indexWriter.close();
      }
      indexWriter = null;
    } catch (Exception e) {
      ParWork.propagateInterrupt("Error during close of writer.", e);
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    }
  }

  @Override
  public RefCounted<IndexWriter> getIndexWriter(SolrCore core) throws IOException {
    return getIndexWriter(core, false);
  }

  @Override
  public RefCounted<IndexWriter> getIndexWriter(SolrCore core, boolean createIndex)
          throws IOException {
    if (core != null && (!core.indexEnabled || core.readOnly)) {
      throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE,
              "Indexing is temporarily disabled");
    }

//    if (core != null && core.getCoreContainer().isShutDown()) {
//      throw new AlreadyClosedException("CoreContainer is already closed");
//    }

    boolean succeeded = false;
    lock(iwLock.readLock());
    try {
      // Multiple readers may be executing this, but we only want one to open the writer on demand.
      iwOpenLock.lock();
      try {
        if (core == null) {
          // core == null is a signal to just return the current writer, or null if none.
          initRefCntWriter();
          if (refCntWriter == null) return null;
        } else {
          if (indexWriter == null) {
            if (core.getCoreContainer().isShutDown()) {
              throw new AlreadyClosedException("CoreContainer is already closed");
            }

            indexWriter = createMainIndexWriter(core, createIndex,"DirectUpdateHandler2");
          }
          initRefCntWriter();
        }

        refCntWriter.incref();
        succeeded = true;  // the returned RefCounted<IndexWriter> will release the readLock on a decref()
        return refCntWriter;
      } finally {
        iwOpenLock.unlock();
      }

    } finally {
      // if we failed to return the IW for some other reason, we should unlock.
      if (!succeeded) {
        iwLock.readLock().unlock();
      }
    }

  }

  private void initRefCntWriter() {
    // TODO: since we moved to a read-write lock, and don't rely on the count to close the writer, we don't really
    // need this class any more.  It could also be a singleton created at the same time as SolrCoreState
    // or we could change the API of SolrCoreState to just return the writer and then add a releaseWriter() call.
    if (refCntWriter == null && indexWriter != null) {
      refCntWriter = new RefCounted<IndexWriter>(indexWriter) {

        @Override
        public void decref() {
          iwLock.readLock().unlock();
          super.decref();  // This is now redundant (since we switched to read-write locks), we don't really need to maintain our own reference count.
        }

        @Override
        public void close() {
          //  We rely on other code to actually close the IndexWriter, and there's nothing special to do when the ref count hits 0
        }
      };
    }
  }

  // acquires the lock or throws an exception if the CoreState has been closed.
  private static void lock(Lock lock) {
    boolean acquired = false;
    do {
      try {
        acquired = lock.tryLock() || lock.tryLock(100, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        log.warn("WARNING - Dangerous interrupt", e);
      }
    } while (!acquired);
  }

  // closes and opens index writers without any locking
  private void changeWriter(SolrCore core, boolean rollback, boolean createIndex, boolean openNewWriter) throws IOException {
    String coreName = core.getName();

    // We need to null this so it picks up the new writer next get call.
    // We do this before anything else in case we hit an exception.
    refCntWriter = null;
    IndexWriter iw = indexWriter; // temp reference just for closing
    indexWriter = null; // null this out now in case we fail, so we won't use the writer again

    if (iw != null) {
      if (!rollback) {
        try {
          log.debug("Closing old IndexWriter... core={}", coreName);
          iw.commit();
          iw.rollback();
        } catch (Exception e) {
          ParWork.propagateInterrupt("Error closing old IndexWriter. core=" + coreName, e);
        }
      } else {
        try {
          log.debug("Rollback old IndexWriter... core={}", coreName);
          iw.rollback();
        } catch (Exception e) {
          ParWork.propagateInterrupt("Error rolling back old IndexWriter. core=" + coreName, e);
        }
      }
    }

    if (openNewWriter) {
      indexWriter = createMainIndexWriter(core, createIndex, "DirectUpdateHandler2");
      log.info("New IndexWriter is ready to be used.");
    }
  }

  @Override
  public void newIndexWriter(SolrCore core, boolean rollback) throws IOException {
    lock(iwLock.writeLock());
    try {
      changeWriter(core, rollback, false, true);
    } finally {
      iwLock.writeLock().unlock();
    }
  }

  @Override
  public void newIndexWriter(SolrCore core, boolean rollback, boolean createIndex) throws IOException {
    lock(iwLock.writeLock());
    try {
      changeWriter(core, rollback, createIndex, true);
    } finally {
      iwLock.writeLock().unlock();
    }
  }

  @Override
  public void closeIndexWriter(SolrCore core, boolean rollback) throws IOException {
    lock(iwLock.writeLock());
    changeWriter(core, rollback, false,false);
    // Do not unlock the writeLock in this method.  It will be unlocked by the openIndexWriter call (see base class javadoc)
  }

  @Override
  public void openIndexWriter(SolrCore core) throws IOException {
    try {
      changeWriter(core, false, false, true);
    } finally {
      iwLock.writeLock().unlock();  //unlock even if we failed
    }
  }

  @Override
  public void rollbackIndexWriter(SolrCore core) throws IOException {
    iwLock.writeLock().lock();
    try {
      changeWriter(core, true, false, true);
    } finally {
      iwLock.writeLock().unlock();
    }
  }

  protected static SolrIndexWriter createMainIndexWriter(SolrCore core, boolean createIndex, String name) throws IOException {
    SolrIndexWriter iw;
    try {
      iw = buildIndexWriter(core, name, core.getNewIndexDir(), core.getDirectoryFactory(), createIndex, core.getLatestSchema(),
              core.getSolrConfig().indexConfig, core.getDeletionPolicy(), core.getCodec(), false);
    } catch (Exception e) {
      ParWork.propagateInterrupt(e);
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    }

    return iw;
  }

  public static SolrIndexWriter buildIndexWriter(SolrCore core, String name, String path, DirectoryFactory directoryFactory, boolean create, IndexSchema schema,
      SolrIndexConfig config, IndexDeletionPolicy delPolicy, Codec codec, boolean commitOnClose) {
    SolrIndexWriter iw;
    Directory dir = null;
    try {
      dir = getDir(directoryFactory, path, config);
      iw = new SolrIndexWriter(core, name, directoryFactory, dir, create, schema, config, delPolicy, codec, commitOnClose);
    } catch (Throwable e) {
      ParWork.propagateInterrupt(e);
      SolrException exp = new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);

      if (dir != null) {
//          try {
//            directoryFactory.release(dir);
//          } catch (IOException e1) {
//            exp.addSuppressed(e1);
//          }
      }
      if (e instanceof  Error) {
        log.error("Exception constructing SolrIndexWriter", exp);
        throw (Error) e;
      }
      throw exp;
    }

    return iw;
  }

  public static Directory getDir(DirectoryFactory directoryFactory, String path, SolrIndexConfig config) {
    Directory dir;
    try {
      dir = directoryFactory.get(path, DirectoryFactory.DirContext.DEFAULT, config.lockType);
    } catch (Exception e) {
      ParWork.propagateInterrupt(e);
      SolrException exp = new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      throw exp;
    }
    return dir;
  }

  public Sort getMergePolicySort() throws IOException {
    lock(iwLock.readLock());
    try {
      if (indexWriter != null) {
        final MergePolicy mergePolicy = indexWriter.getConfig().getMergePolicy();
        if (mergePolicy instanceof SortingMergePolicy) {
          return ((SortingMergePolicy)mergePolicy).getSort();
        }
      }
    } finally {
      iwLock.readLock().unlock();
    }
    return null;
  }

  @Override
  public DirectoryFactory getDirectoryFactory() {
    return directoryFactory;
  }

  @Override
  public RecoveryStrategy.Builder getRecoveryStrategyBuilder() {
    return recoveryStrategyBuilder;
  }

  @Override
  public void doRecovery(CoreContainer cc, CoreDescriptor cd, String source) {
    try (SolrCore core = cc.getCore(cd.getName())) {
      doRecovery(core, source);
    }
  }

  @Override
  public void doRecovery(SolrCore core, String source) {

    log.info("Do recovery for core {} source={}", core.getName(), source);
    CoreContainer corecontainer = core.getCoreContainer();
    if (prepForClose || closed || corecontainer.isShutDown()) {
      log.warn("Skipping recovery because Solr is shutdown");
      return;
    }

    try {
      // we make recovery requests async - that async request may
      // have to 'wait in line' a bit or bail if a recovery is
      // already queued up - the recovery execution itself is run
      // in another thread on another 'recovery' executor.
      //
      if (log.isDebugEnabled()) log.debug("Submit recovery for {}", core.getName());

      Boolean newVal = needsRecovery.getAndUpdate(aBoolean -> {
        if (recoveryRunning) {
          log.info("Recovery already running core {}", core.getName());
          cancelRecovery();
          return true;
        }
        recoveryRunning = true;
        log.info("Running recovery for core {}", core.getName());
        RecoveryTask recoveryTask = new RecoveryTask(core, corecontainer, needsRecovery);
        CompletableFuture.runAsync(recoveryTask, corecontainer.getUpdateShardHandler().getRecoveryExecutor());
        return false;
      });

    } catch (RejectedExecutionException e) {
      // fine, we are shutting down
      log.warn("Skipping recovery because we are closed");
    }
  }

  @Override
  public void cancelRecovery() {
    cancelRecovery(false, false);
  }

  @Override
  public void cancelRecovery(boolean wait, boolean prepForClose) {
    if (log.isDebugEnabled()) log.debug("Cancel recovery");
    
    if (prepForClose) {
      this.prepForClose = true;
    }

    if (recoveryStrat != null) {
      try {
        recoveryStrat.close();
      } catch (NullPointerException e) {
        // okay
      }
    }

    recoveryStrat = null;
  }

  /** called from recoveryStrat on a successful recovery */
  @Override
  public void recovered() {
    recoveringAfterStartup = false;  // once we have successfully recovered, we no longer need to act as if we are recovering after startup
  }

  public boolean isRecoverying() {
    return recoveryRunning;
  }


  /** called from recoveryStrat on a failed recovery */
  @Override
  public void failed() {

  }

  @Override
  public void close(IndexWriterCloser closer) {

    // we can't lock here without
    // a blocking race, we should not need to
    // though
    // iwLock.writeLock().lock();

    log.debug("Closing SolrCoreState refCnt={}", solrCoreStateRefCnt.get());

    cancelRecovery(false, true);

    try {
      closeIndexWriter(closer);
    } finally {
      // iwLock.writeLock().unlock();
      IOUtils.closeQuietly(directoryFactory);
    }

  }

  @Override
  public Lock getCommitLock() {
    return commitLock;
  }

  @Override
  public boolean getLastReplicateIndexSuccess() {
    return lastReplicationSuccess;
  }

  @Override
  public void setLastReplicateIndexSuccess(boolean success) {
    this.lastReplicationSuccess = success;
  }

  private class RecoveryTask implements Runnable {
    private final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final SolrCore core;
    private final CoreContainer corecontainer;
    private final AtomicReference<Boolean> needsRecovery;

    public RecoveryTask(SolrCore core, CoreContainer corecontainer, AtomicReference<Boolean> needsRecoveryRef) {
      this.core = core;
      this.corecontainer = corecontainer;
      this.needsRecovery = needsRecoveryRef;
    }

    @Override public void run() {
      CoreDescriptor coreDescriptor = core.getCoreDescriptor();
      MDCLoggingContext.setCoreName(core.getName());
      MDCLoggingContext.setNode(corecontainer.getZkController().getNodeName());
      try {
        if (SKIP_AUTO_RECOVERY) {
          log.warn("Skipping recovery according to sys prop solrcloud.skip.autorecovery");
          return;
        }

        if (log.isDebugEnabled()) log.debug("Going to create and run RecoveryStrategy");

        // check before we grab the lock
        if (prepForClose || closed || corecontainer.isShutDown()) {
          log.warn("Skipping recovery because Solr is shutdown");
          return;
        }

        recoveryThrottle.minimumWaitBetweenActions();
        recoveryThrottle.markAttemptingAction();

        recoveryStrat = recoveryStrategyBuilder.create(corecontainer, coreDescriptor, DefaultSolrCoreState.this);
        recoveryStrat.setRecoveringAfterStartup(recoveringAfterStartup);

        recoveryStrat.run();

        if (log.isDebugEnabled()) log.debug("Running recovery");


      } catch (AlreadyClosedException e) {
        log.warn("Skipping recovery because we are closed");
      } catch (Exception e) {
        log.error("Exception starting recovery", e);
      } finally {

        needsRecovery.getAndUpdate(aBoolean -> {
          if (!aBoolean) {
            return false;
          }
          recoveryRunning = true;
          recoveryStrat.unclose();
          CompletableFuture.runAsync(this, corecontainer.getUpdateShardHandler().getRecoveryExecutor());
          return false;
        });

        MDCLoggingContext.clear();
      }
    }
  }
}
