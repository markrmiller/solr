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
package org.apache.solr.cloud;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest.WaitForState;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.util.AsyncListener;
import org.apache.solr.client.solrj.util.Cancellable;
import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.IndexFetcher;
import org.apache.solr.handler.ReplicationHandler;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.PeerSync;
import org.apache.solr.update.PeerSyncWithLeader;
import org.apache.solr.update.UpdateLog;
import org.apache.solr.update.UpdateLog.RecoveryInfo;
import org.apache.solr.update.processor.DistributedUpdateProcessor;
import org.apache.solr.util.RefCounted;
import org.apache.solr.util.SolrPluginUtils;
import org.apache.solr.util.TimeOut;
import org.apache.solr.util.plugin.NamedListInitializedPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class may change in future and customisations are not supported between versions in terms of API or back compat
 * behaviour.
 *
 * @lucene.experimental
 */
public class RecoveryStrategy implements Runnable, Closeable {

  private volatile ReplicationHandler replicationHandler;
  private final Http2SolrClient recoveryOnlyClient;
  private volatile boolean firstTime = true;

  final public void unclose() {
    close = false;
  }

  public static class Builder implements NamedListInitializedPlugin {
    private NamedList args;

    @Override
    public void init(NamedList args) {
      this.args = args;
    }

    // this should only be used from SolrCoreState
    public RecoveryStrategy create(CoreContainer cc, CoreDescriptor cd,
        RecoveryStrategy.RecoveryListener recoveryListener) {
      final RecoveryStrategy recoveryStrategy = newRecoveryStrategy(cc, cd, recoveryListener);
      SolrPluginUtils.invokeSetters(recoveryStrategy, args);
      return recoveryStrategy;
    }

    protected RecoveryStrategy newRecoveryStrategy(CoreContainer cc, CoreDescriptor cd,
        RecoveryStrategy.RecoveryListener recoveryListener) {
      return new RecoveryStrategy(cc, cd, recoveryListener);
    }
  }

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private volatile int waitForUpdatesWithStaleStatePauseMilliSeconds = Integer
      .getInteger("solr.cloud.wait-for-updates-with-stale-state-pause", 0);
  private volatile int maxRetries = Integer.getInteger("solr.recovery.maxretries", 500);
  private volatile int startingRecoveryDelayMilliSeconds = Integer
      .getInteger("solr.cloud.starting-recovery-delay-milli-seconds", 100);

  public interface RecoveryListener {
    public void recovered();

    public void failed();
  }

  private volatile boolean close = false;
  private final RecoveryListener recoveryListener;
  private final ZkController zkController;
  private final String baseUrl;
  private final ZkStateReader zkStateReader;
  private final String coreName;
  private final AtomicInteger retries = new AtomicInteger(0);
  private boolean recoveringAfterStartup;
  private volatile Cancellable prevSendPreRecoveryRequest;
  private volatile Replica.Type replicaType;

  private final CoreContainer cc;

  protected RecoveryStrategy(CoreContainer cc, CoreDescriptor cd, RecoveryListener recoveryListener) {
    // ObjectReleaseTracker.track(this);
    this.cc = cc;
    this.coreName = cd.getName();
    String collection = cd.getCloudDescriptor().getCollectionName();
    String shard = cd.getCloudDescriptor().getShardId();

    this.recoveryListener = recoveryListener;
    zkController = cc.getZkController();
    zkStateReader = zkController.getZkStateReader();
    baseUrl = zkController.getBaseUrl();

    recoveryOnlyClient = cc.getUpdateShardHandler().getRecoveryOnlyClient();
  }

  final public int getWaitForUpdatesWithStaleStatePauseMilliSeconds() {
    return waitForUpdatesWithStaleStatePauseMilliSeconds;
  }

  final public void setWaitForUpdatesWithStaleStatePauseMilliSeconds(
      int waitForUpdatesWithStaleStatePauseMilliSeconds) {
    this.waitForUpdatesWithStaleStatePauseMilliSeconds = waitForUpdatesWithStaleStatePauseMilliSeconds;
  }

  final public int getMaxRetries() {
    return maxRetries;
  }

  final public void setMaxRetries(int maxRetries) {
    this.maxRetries = maxRetries;
  }

  final public int getStartingRecoveryDelayMilliSeconds() {
    return startingRecoveryDelayMilliSeconds;
  }

  final public void setStartingRecoveryDelayMilliSeconds(int startingRecoveryDelayMilliSeconds) {
    this.startingRecoveryDelayMilliSeconds = startingRecoveryDelayMilliSeconds;
  }

  final public boolean getRecoveringAfterStartup() {
    return recoveringAfterStartup;
  }

  final public void setRecoveringAfterStartup(boolean recoveringAfterStartup) {
    this.recoveringAfterStartup = recoveringAfterStartup;
  }

  // make sure any threads stop retrying
  @Override
  final public void close() {
    close = true;

    if (log.isDebugEnabled()) log.debug("Stopping recovery for core=[{}]", coreName);


    try {
      if (prevSendPreRecoveryRequest != null) {
       // prevSendPreRecoveryRequest.cancel();
      }
      prevSendPreRecoveryRequest = null;
    } catch (Exception e) {
      // expected
    }

    ReplicationHandler finalReplicationHandler = replicationHandler;
    if (finalReplicationHandler != null) {

      finalReplicationHandler.abortFetch();
    }

    //IOUtils.closeQuietly(recoveryOnlyClient);

    //ObjectReleaseTracker.release(this);
  }

  final private void recoveryFailed(final ZkController zkController, final String baseUrl, final CoreDescriptor cd) throws Exception {
    SolrException.log(log, "Recovery failed - I give up.");
    try {
      if (zkController.getZkClient().isAlive()) {
        zkController.publish(cd, Replica.State.RECOVERY_FAILED);
      }
    } finally {
      close();
      recoveryListener.failed();
    }
  }

  /**
   * This method may change in future and customisations are not supported between versions in terms of API or back
   * compat behaviour.
   *
   * @lucene.experimental
   */
  protected String getReplicateLeaderUrl(Replica leaderprops, ZkStateReader zkStateReader) {
    return leaderprops.getCoreUrl();
  }

  final private IndexFetcher.IndexFetchResult replicate(Replica leader)
      throws SolrServerException, IOException {

    log.info("Attempting to replicate from [{}].", leader);

    String leaderUrl;
    // send commit
    try {
      leaderUrl = leader.getCoreUrl();
      commitOnLeader(leaderUrl);
    } catch (Exception e) {
      if (e instanceof  SolrException && ((SolrException) e).getRootCause() instanceof RejectedExecutionException) {
       throw new AlreadyClosedException("An executor is shutdown already");
      }

      log.error("Commit on leader failed", e);
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    }

    ModifiableSolrParams solrParams = new ModifiableSolrParams();
    solrParams.set(ReplicationHandler.MASTER_URL, leaderUrl);
    solrParams.set(ReplicationHandler.SKIP_COMMIT_ON_MASTER_VERSION_ZERO, replicaType == Replica.Type.TLOG);
    // always download the tlogs from the leader when running with cdcr enabled. We need to have all the tlogs
    // to ensure leader failover doesn't cause missing docs on the target


    log.info("do replication fetch [{}].", solrParams);

    return replicationHandler.doFetch(solrParams, retries.get() > 3);

    // solrcloud_debug
//    if (log.isDebugEnabled()) {
//      try {
//        RefCounted<SolrIndexSearcher> searchHolder = core
//            .getNewestSearcher(false);
//        SolrIndexSearcher searcher = searchHolder.get();
//        Directory dir = core.getDirectoryFactory().get(core.getIndexDir(), DirContext.META_DATA, null);
//        try {
//          final IndexCommit commit = core.getDeletionPolicy().getLatestCommit();
//          if (log.isDebugEnabled()) {
//            log.debug("{} replicated {} from {} gen: {} data: {} index: {} newIndex: {} files: {}"
//                , core.getCoreContainer().getZkController().getNodeName()
//                , searcher.count(new MatchAllDocsQuery())
//                , leaderUrl
//                , (null == commit ? "null" : commit.getGeneration())
//                , core.getDataDir()
//                , core.getIndexDir()
//                , core.getNewIndexDir()
//                , Arrays.asList(dir.listAll()));
//          }
//        } finally {
//          core.getDirectoryFactory().release(dir);
//          searchHolder.decref();
//        }
//      } catch (Exception e) {
//        ParWork.propagateInterrupt(e);
//        log.debug("Error in solrcloud_debug block", e);
//      }
//    }

  }

  final private void commitOnLeader(String leaderUrl) throws SolrServerException,
      IOException {

    UpdateRequest ureq = new UpdateRequest();
    ureq.setBasePath(leaderUrl);
    ureq.setParams(new ModifiableSolrParams());
    ureq.getParams().set(DistributedUpdateProcessor.COMMIT_END_POINT, "terminal");
    ureq.getParams().set(UpdateParams.OPEN_SEARCHER, true); // opensearcher=true to ensure we have it for replicate

    log.debug("send commit to leader {} {}", leaderUrl, ureq.getParams());
    ureq.setAction(AbstractUpdateRequest.ACTION.COMMIT, false, false).process(recoveryOnlyClient);
    log.debug("done send commit to leader {}", leaderUrl);
  }

  @Override
  final public void run() {
    // set request info for logging
    log.debug("Starting recovery process. recoveringAfterStartup={}", recoveringAfterStartup);
    try {
      try (SolrCore core = cc.getCore(coreName)) {
        if (core == null) {
          log.warn("SolrCore is null, won't do recovery");
          throw new AlreadyClosedException("SolrCore is null, won't do recovery");
        }

        CoreDescriptor coreDescriptor = core.getCoreDescriptor();
        replicaType = coreDescriptor.getCloudDescriptor().getReplicaType();

        SolrRequestHandler handler = core.getRequestHandler(ReplicationHandler.PATH);
        replicationHandler = (ReplicationHandler) handler;

        doRecovery(core, coreDescriptor);
      }
    } catch (InterruptedException e) {
      log.info("InterruptedException, won't do recovery", e);
    } catch (AlreadyClosedException e) {
      log.info("AlreadyClosedException, won't do recovery", e);
    } catch (RejectedExecutionException e) {
      log.info("RejectedExecutionException, won't do recovery", e);
    } catch (Exception e) {
      ParWork.propagateInterrupt(e);
      log.error("Exception during recovery", e);
    }
  }

  final public void doRecovery(SolrCore core, CoreDescriptor coreDescriptor) throws Exception {

    int tries = 0;
    while (true) {
      try {
        final boolean closed = isClosed();

        final boolean closing = core.isClosing();

        final boolean coreContainerClosed = cc.isShutDown();

        if (closed || closing || coreContainerClosed) {
          close = true;
          return;
        }

        Replica leader = null;
        if (tries == 0) {

          UpdateLog ulog = core.getUpdateHandler().getUpdateLog();

          LeaderElector leaderElector = zkController.getLeaderElector(coreName);

          if (leaderElector != null && leaderElector.isLeader()) {
            log.warn("We are the leader, STOP recovery", new SolrException(ErrorCode.INVALID_STATE, "Leader in recovery"));
            ZkNodeProps zkNodes = ZkNodeProps
                .fromKeyVals(StatePublisher.OPERATION, OverseerAction.STATE.toLower(), ZkStateReader.CORE_NAME_PROP, core.getName(), "id",
                    core.getCoreDescriptor().getCoreProperty("collId", null) + "-" + core.getCoreDescriptor().getCoreProperty("id", null),
                    ZkStateReader.COLLECTION_PROP, core.getCoreDescriptor().getCollectionName(), ZkStateReader.STATE_PROP, Replica.State.LEADER);
            zkController.publish(zkNodes);
            close = true;
            return;
          }

          log.debug("Publishing state of core [{}] as buffering {}", coreName, "doSyncOrReplicateRecovery");

          zkController.publish(core.getCoreDescriptor(), Replica.State.BUFFERING);

          //log.debug("Begin buffering updates. core=[{}]", coreName);
          // recalling buffer updates will drop the old buffer tlog
          //if (ulog.getState() != UpdateLog.State.BUFFERING) {
          // ulog.bufferUpdates();
          //}

           leader = zkController.getZkStateReader().getLeaderRetry(recoveryOnlyClient, coreDescriptor.getCollectionName(), coreDescriptor.getCloudDescriptor().getShardId(),
              Integer.getInteger("solr.getleader.looptimeout", 8000), true);

          boolean success = sendPrepRecoveryCmd(leader, core.getCoreDescriptor());
          if (!success) {
            waitForRetry(core);
            continue;
          }
        } else {
          tries++;
        }

        if (tries > 1) {
          waitForRetry(core);
        }

        LeaderElector leaderElector = zkController.getLeaderElector(coreName);

        if (leaderElector != null && leaderElector.isLeader()) {
          log.warn("We are the leader, STOP recovery", new SolrException(ErrorCode.INVALID_STATE, "Leader in recovery"));
          ZkNodeProps zkNodes = ZkNodeProps
              .fromKeyVals(StatePublisher.OPERATION, OverseerAction.STATE.toLower(), ZkStateReader.CORE_NAME_PROP, core.getName(), "id",
                  core.getCoreDescriptor().getCoreProperty("collId", null) + "-" + core.getCoreDescriptor().getCoreProperty("id", null),
                  ZkStateReader.COLLECTION_PROP, core.getCoreDescriptor().getCollectionName(), ZkStateReader.STATE_PROP, Replica.State.LEADER);
          zkController.publish(zkNodes);
          close = true;
          return;
        }

        if (tries > 1) {
          leader = zkController.getZkStateReader()
              .getLeaderRetry(recoveryOnlyClient, coreDescriptor.getCollectionName(), coreDescriptor.getCloudDescriptor().getShardId(), Integer.getInteger("solr.getleader.looptimeout", 8000), true);
        }

        if (leaderElector != null && leaderElector.isLeader()) {
          log.warn("We are the leader, STOP recovery", new SolrException(ErrorCode.INVALID_STATE, "Leader in recovery"));
          //          ZkNodeProps zkNodes = ZkNodeProps
          //              .fromKeyVals(StatePublisher.OPERATION, OverseerAction.STATE.toLower(), ZkStateReader.CORE_NAME_PROP, core.getName(), "id",
          //                  core.getCoreDescriptor().getCoreProperty("collId", null)+ "-" +  core.getCoreDescriptor().getCoreProperty("id", null),
          //                  ZkStateReader.COLLECTION_PROP, core.getCoreDescriptor().getCollectionName(), ZkStateReader.STATE_PROP, Replica.State.LEADER);
          //          zkController.publish(zkNodes);
          close = true;
          return;
        }

        if (leader != null && leader.getName().equals(coreName)) {
          log.warn("We are the leader in cluster state, REPEAT recovery");
          Thread.sleep(50);
          continue;
        }

        if (core.getCoreContainer().isShutDown()) {
          log.info("We are closing, STOP recovery");
          close = true;
          return;
        }

        boolean successfulRecovery;
        if (coreDescriptor.getCloudDescriptor().requiresTransactionLog()) {
          if (log.isDebugEnabled()) log.debug("Sync or replica recovery");
          successfulRecovery = doSyncOrReplicateRecovery(core, leader);
        } else {
          if (log.isDebugEnabled()) log.debug("Replicate only recovery");
          successfulRecovery = doReplicateOnlyRecovery(core, leader);
        }

        if (successfulRecovery) {
          close = true;
          break;
        } else {
          final boolean coreClosed = core.isClosing() || core.isClosing();
          if (isClosed() || coreClosed || core.getCoreContainer().isShutDown()) {
            log.info("We are already coreClosed, stopping recovery");
            close = true;
            return;
          }

          log.info("Trying another loop to recover after failing try={}", tries);
        }

      } catch (Exception e) {
        if (close) {
          return;
        }
        log.info("Exception trying to recover, try again try={}", tries, e);
      }
    }
  }

  final private boolean doReplicateOnlyRecovery(SolrCore core, Replica leader) throws Exception {
    boolean successfulRecovery = false;

    // if (core.getUpdateHandler().getUpdateLog() != null) {
    // SolrException.log(log, "'replicate-only' recovery strategy should only be used if no update logs are present, but
    // this core has one: "
    // + core.getUpdateHandler().getUpdateLog());
    // return;
    // }

    try {
      if (isClosed()) {
        throw new AlreadyClosedException();
      }
      log.info("Starting Replication Recovery. [{}] leader is [{}] and I am [{}]", coreName, leader.getName(), Replica.getCoreUrl(baseUrl, coreName));

      try {
        log.info("Stopping background replicate from leader process");
        zkController.stopReplicationFromLeader(coreName);
        IndexFetcher.IndexFetchResult result = replicate(leader);

        if (result.getSuccessful()) {
          log.info("replication fetch reported as success");
        } else {
          log.error("replication fetch reported as failed: {} {}", result.getMessage(), result, result.getException());
          successfulRecovery = false;
          throw new SolrException(ErrorCode.SERVER_ERROR, "Replication fetch reported as failed");
        }

        log.info("Replication Recovery was successful.");
        successfulRecovery = true;
      } catch (Exception e) {
        log.error("Error while trying to recover", e);
        successfulRecovery = false;
      }

    } catch (Exception e) {
      log.error("Error while trying to recover. core={}", coreName, e);
      successfulRecovery = false;
    } finally {
      if (successfulRecovery) {
        log.info("Restarting background replicate from leader process");
        zkController.startReplicationFromLeader(coreName, false);
        log.debug("Registering as Active after recovery.");
        try {
          zkController.publish(core.getCoreDescriptor(), Replica.State.ACTIVE);
        } catch (Exception e) {
          log.error("Could not publish as ACTIVE after succesful recovery", e);
          successfulRecovery = false;
        }

        if (successfulRecovery) {
          recoveryListener.recovered();
        }
      }
    }

    if (!successfulRecovery) {
      // lets pause for a moment and we need to try again...
      // TODO: we don't want to retry for some problems?
      // Or do a fall off retry...
      try {

        log.error("Recovery failed - trying again... ({})", retries);

        if (retries.incrementAndGet() >= maxRetries) {
          close = true;
          log.error("Recovery failed - max retries exceeded ({}).", retries);
          try {
            recoveryFailed(zkController, baseUrl, core.getCoreDescriptor());
          } catch (InterruptedException e) {

          } catch (Exception e) {
            log.error("Could not publish that recovery failed", e);
          }
        }
      } catch (Exception e) {
        log.error("An error has occurred during recovery", e);
      }
    }

    // We skip core.seedVersionBuckets(); We don't have a transaction log
    if (successfulRecovery) {
      close = true;
    }

    log.info("Finished recovery process, successful=[{}]", successfulRecovery);

    return successfulRecovery;
  }

  // TODO: perhaps make this grab a new core each time through the loop to handle core reloads?
  public final boolean doSyncOrReplicateRecovery(SolrCore core, Replica leader) throws Exception {
    log.debug("Do peersync or replication recovery core={} collection={}", coreName, core.getCoreDescriptor().getCollectionName());

    boolean successfulRecovery = false;
    boolean publishedActive = false;
    UpdateLog ulog;

    ulog = core.getUpdateHandler().getUpdateLog();

    // we temporary ignore peersync for tlog replicas
    if (firstTime) {
      firstTime = replicaType != Replica.Type.TLOG;
    }

    boolean didReplication = false;

    List<Long> recentVersions;
    try (UpdateLog.RecentUpdates recentUpdates = ulog.getRecentUpdates()) {
      recentVersions = recentUpdates.getVersions(ulog.getNumRecordsToKeep());
    } catch (Exception e) {
      log.error("Corrupt tlog - ignoring.", e);
      recentVersions = Collections.emptyList();
    }

    List<Long> startingVersions = ulog.getStartingVersions();

    if (startingVersions != null && recentVersions.size() > 0 && recoveringAfterStartup) {
      try {
        int oldIdx = 0; // index of the start of the old list in the current list
        long firstStartingVersion = startingVersions.size() > 0 ? startingVersions.get(0) : 0;

        final int size = recentVersions.size();
        for (; oldIdx < size; oldIdx++) {
          if (recentVersions.get(oldIdx) == firstStartingVersion) break;
        }

        if (oldIdx > 0) {
          log.info("Found new versions added after startup: num=[{}]", oldIdx);
          if (log.isInfoEnabled()) {
            log.info("currentVersions size={} range=[{} to {}]", size, recentVersions.get(0), recentVersions.get(size - 1));
          }
        }

        if (startingVersions.isEmpty()) {
          log.debug("startupVersions is empty");
        } else {
          if (log.isDebugEnabled()) {
            log.debug("startupVersions size={} range=[{} to {}]", startingVersions.size(), startingVersions.get(0),
                startingVersions.get(startingVersions.size() - 1));
          }
        }
      } catch (Exception e) {
        log.error("Error getting recent versions.", e);
        recentVersions = Collections.emptyList();
      }
    }

    if (recoveringAfterStartup) {
      // if we're recovering after startup (i.e. we have been down), then we need to know what the last versions were
      // when we went down. We may have received updates since then.
      recentVersions = startingVersions;
      try {
        if (ulog.existOldBufferLog()) {
          // this means we were previously doing a full index replication
          // that probably didn't complete and buffering updates in the
          // meantime.
          log.info("Looks like a previous replication recovery did not complete - skipping peer sync.");
          firstTime = false; // skip peersync
        }
      } catch (Exception e) {
        ParWork.propagateInterrupt(e);
        SolrException.log(log, "Error trying to get ulog starting operation.", e);
        firstTime = false; // skip peersync
      }
    }

    if (replicaType == Replica.Type.TLOG) {
      log.debug("Stopping replication from leader for {}", coreName);
      zkController.stopReplicationFromLeader(coreName);
    }

    LeaderElector leaderElector = zkController.getLeaderElector(coreName);

    if (leaderElector != null && leaderElector.isLeader()) {
      log.warn("We are the leader, STOP recovery", new SolrException(ErrorCode.INVALID_STATE, "Leader in recovery"));
      //      ZkNodeProps zkNodes = ZkNodeProps
      //          .fromKeyVals(StatePublisher.OPERATION, OverseerAction.STATE.toLower(), ZkStateReader.CORE_NAME_PROP, core.getName(), "id",
      //              core.getCoreDescriptor().getCoreProperty("collId", null)+ "-" +  core.getCoreDescriptor().getCoreProperty("id", null),
      //              ZkStateReader.COLLECTION_PROP, core.getCoreDescriptor().getCollectionName(), ZkStateReader.STATE_PROP, Replica.State.LEADER);
      //      zkController.publish(zkNodes);
      close = true;
      return false;
    }

    try {

      // first thing we just try to sync
      if (firstTime) {
        firstTime = false; // only try sync the first time through the loop

        log.debug("Attempting to PeerSync from [{}] - recoveringAfterStartup=[{}]", leader.getCoreUrl(), recoveringAfterStartup);

        // System.out.println("Attempting to PeerSync from " + leaderUrl
        // + " i am:" + zkController.getNodeName());
        try {
          boolean syncSuccess;
          boolean noNewSearcher = false;
          try (PeerSyncWithLeader peerSyncWithLeader = new PeerSyncWithLeader(core, leader.getCoreUrl(), ulog.getNumRecordsToKeep())) {
            PeerSync.PeerSyncResult syncResult = peerSyncWithLeader.sync(recentVersions);
            syncSuccess = syncResult.isSuccess();

            if (!syncSuccess && recentVersions.size() == 0 && syncResult.getOtherHasVersions().isPresent() && !syncResult.getOtherHasVersions().get()) {
              syncSuccess = true;
            }
          }

          if (syncSuccess) {
            // solrcloud_debug
            // cloudDebugLog(core, "synced");
            if (isClosed() || core.isClosed() || core.getCoreContainer().isShutDown()) {
              log.info("Bailing on recovery due to close");
              close = true;
              return false;
            }
            // log.debug("Replaying updates buffered during PeerSync.");
            //ulog.bufferUpdates();
            // exit buffering state
            replay(core);

            // sync success
            successfulRecovery = true;
          } else {
            successfulRecovery = false;
          }

        } catch (Exception e) {
          log.error("PeerSync exception", e);
          successfulRecovery = false;
        }

        if (!successfulRecovery) {
          log.info("PeerSync Recovery was not successful - trying replication.");
        }
      }
      if (!successfulRecovery) {
        log.info("Starting Replication Recovery.");
        didReplication = true;
        try {

          if (isClosed() || core.isClosed() || core.getCoreContainer().isShutDown()) {
            log.info("Bailing on recovery due to close");
            close = true;
            return false;
          }
          // recalling buffer updates will drop the old buffer tlog
          //if (ulog.getState() != UpdateLog.State.BUFFERING) {
            ulog.bufferUpdates();
         // }

          log.debug("Begin buffering updates. core=[{}]", coreName);

          IndexFetcher.IndexFetchResult result = replicate(leader);

          if (result.getSuccessful()) {
            log.info("replication fetch reported as success");

            replay(core);

            log.info("Replication Recovery was successful.");
            successfulRecovery = true;
          } else {
            log.error("replication fetch reported as failed: {} {}", result.getMessage(), result, result.getException());
            successfulRecovery = false;
          }

          if (isClosed() || core.isClosed() || core.getCoreContainer().isShutDown()) {
            log.info("Bailing on recovery due to close");
            close = true;
            return false;
          }

        } catch (InterruptedException | AlreadyClosedException | RejectedExecutionException e) {
          log.info("{} bailing on recovery", e.getClass().getSimpleName());
          close = true;
          return false;
        } catch (Exception e) {
          successfulRecovery = false;
          log.error("Error while trying to recover", e);
        }
      }
    } catch (Exception e) {
      log.error("Error while trying to recover. core={}", coreName, e);
      successfulRecovery = false;
    }
    if (successfulRecovery) {
      log.info("Registering as Active after recovery {}", coreName);
      try {
        // if replay was skipped (possibly to due pulling a full index from the leader),
        // then we still need to update version bucket seeds after recovery
        if (didReplication) {
          if (isClosed() || core.isClosed() || core.getCoreContainer().isShutDown()) {
            log.info("Bailing on recovery due to close");
            return false;
          }
          log.info("Updating version bucket highest from index after successful recovery.");
          try {
            core.seedVersionBuckets();
          } catch (Exception e) {
            log.error("Exception seeding version buckets");
          }
        }

        if (replicaType == Replica.Type.TLOG) {
          zkController.startReplicationFromLeader(coreName, true);
        }

        if (isClosed() || core.isClosed() || core.getCoreContainer().isShutDown()) {
          log.info("Bailing on recovery due to close");
          return false;
        }
        log.info("Publishing as ACTIVE after successful recovery");
        zkController.publish(core.getCoreDescriptor(), Replica.State.ACTIVE);
        recoveryListener.recovered();
        return true;
      } catch (AlreadyClosedException | RejectedExecutionException e) {
        log.error("Already closed");
        close = true;
        return false;
      } catch (Exception e) {
        log.error("Could not publish as ACTIVE after successful recovery", e);
        successfulRecovery = false;
      }

    } else {
      log.info("Recovery was not successful, will not register as ACTIVE {}", coreName);
    }

    if (!isClosed()) {
      // lets pause for a moment and we need to try again...
      // TODO: we don't want to retry for some problems?
      // Or do a fall off retry...
      try {

        if (close || cc.isShutDown() || core.isClosing() || core.isClosed()) {
          close = true;
          throw new AlreadyClosedException();
        }
        log.error("Recovery failed - trying again... ({})", retries);

        if (retries.incrementAndGet() >= maxRetries) {
          SolrException.log(log, "Recovery failed - max retries exceeded (" + retries + ").");
          close = true;
          try {
            recoveryFailed(zkController, baseUrl, core.getCoreDescriptor());
          } catch (InterruptedException e) {

          } catch (Exception e) {
            log.error("Could not publish that recovery failed", e);
          }
        }
      } catch (Exception e) {
        log.error("An error has occurred during recovery", e);
      }
    }

    if (!isClosed() && !core.isClosing() && !core.isClosed()) {
      waitForRetry(core);
    } else {
      close = true;
      return false;
    }

    log.debug("Finished doSyncOrReplicateRecovery process, successful=[{}]", successfulRecovery);

    return false;
  }

  private final void waitForRetry(SolrCore core) {
    try {
      if (close) throw new AlreadyClosedException();
      long wait;

      if (retries.get() >= 0 && retries.get() < 3) {
        wait = this.startingRecoveryDelayMilliSeconds;
      } else if (retries.get() < 5) {
        wait = 1000;
      } else {
        wait = 15000;
      }

      log.info("Wait [{}] ms before trying to recover again (attempt={})", wait, retries);

      TimeOut timeout = new TimeOut(wait, TimeUnit.MILLISECONDS, TimeSource.NANO_TIME);
      while (true) {
        final boolean hasTimedOut = timeout.hasTimedOut();
        if (hasTimedOut) break;
        if (isClosed() && !core.isClosing() && !core.isClosed()) {
          log.info("RecoveryStrategy has been closed");
          return;
        }
        if (wait > 1000) {
          Thread.sleep(1000);
        } else {
          Thread.sleep(wait);
        }
      }

    } catch (InterruptedException e) {

    }

  }

  public static Runnable testing_beforeReplayBufferingUpdates;

  private void replay(SolrCore core)
      throws InterruptedException, ExecutionException {
    if (testing_beforeReplayBufferingUpdates != null) {
      testing_beforeReplayBufferingUpdates.run();
    }

    if (replicaType == Replica.Type.TLOG) {
      // roll over all updates during buffering to new tlog, make RTG available
      try (SolrQueryRequest req = new LocalSolrQueryRequest(core, new ModifiableSolrParams())) {
        core.getUpdateHandler().getUpdateLog().copyOverBufferingUpdates(new CommitUpdateCommand(req, false));
      }
    }
    Future<RecoveryInfo> future = core.getUpdateHandler().getUpdateLog().applyBufferedUpdates();
    if (future == null) {
      // no replay needed\
      log.debug("No replay needed.");
      return;
    } else {
      log.info("Replaying buffered documents.");
      // wait for replay
      RecoveryInfo report;
      try {
        report = future.get(10, TimeUnit.MINUTES); // MRM TODO: - how long? make configurable too
      } catch (InterruptedException e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Replay failed");
      } catch (TimeoutException e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, e);
      }
      if (report.failed) {
        SolrException.log(log, "Replay failed");
        throw new SolrException(ErrorCode.SERVER_ERROR, "Replay failed");
      }
    }

    // the index may ahead of the tlog's caches after recovery, by calling this tlog's caches will be purged
    UpdateLog ulog = core.getUpdateHandler().getUpdateLog();
    if (ulog != null) {
      ulog.openRealtimeSearcher();
    }

    // solrcloud_debug
    // cloudDebugLog(core, "replayed");
  }

  static private void cloudDebugLog(SolrCore core, String op) {
    if (!log.isDebugEnabled()) {
      return;
    }
    try {
      RefCounted<SolrIndexSearcher> searchHolder = core.getNewestSearcher(false);
      SolrIndexSearcher searcher = searchHolder.get();
      try {
        final int totalHits = searcher.count(new MatchAllDocsQuery());
        final String nodeName = core.getCoreContainer().getZkController().getNodeName();
        log.debug("[{}] {} [{} total hits]", nodeName, op, totalHits);
      } finally {
        searchHolder.decref();
      }
    } catch (Exception e) {
      ParWork.propagateInterrupt(e);
      log.debug("Error in solrcloud_debug block", e);
    }
  }

  final public boolean isClosed() {
    return close || cc.isShutDown();
  }

  final private boolean sendPrepRecoveryCmd(Replica leader, CoreDescriptor coreDescriptor) {

    if (isClosed()) {
      throw new AlreadyClosedException();
    }

    String leaderCoreName = leader.getName();

    if (leader.getNodeName().equals(zkController.getNodeName())) {
      if (!zkStateReader.isLocalLeader.isLocalLeader(leaderCoreName)) {
        throw new IllegalStateException("Via local check, " + leaderCoreName + " is not a current valid leader");
      }
    }
    String leaderBaseUrl = leader.getBaseUrl();
    WaitForState prepCmd = new WaitForState();
    prepCmd.setCoreName(coreName);
    prepCmd.setLeaderName(leaderCoreName);
    prepCmd.setState(Replica.State.BUFFERING);
    prepCmd.setCoresCollection(coreDescriptor.getCollectionName());
    prepCmd.setCheckIsLeader(true);

    log.info("Sending prep recovery command to {} for leader={} params={}", leaderBaseUrl, leaderCoreName, prepCmd.getParams());

    try (Http2SolrClient client = new Http2SolrClient.Builder(leaderBaseUrl).withHttpClient(cc.getUpdateShardHandler()
        .getRecoveryOnlyClient()).markInternalRequest().build()) {

      prepCmd.setBasePath(leaderBaseUrl);
      CountDownLatch latch = new CountDownLatch(1);
      AtomicReference<NamedList> results = new AtomicReference<>();
      AtomicReference<Throwable> exp = new AtomicReference<>();
      prevSendPreRecoveryRequest = client.asyncRequest(prepCmd, null, new PrepRecoveryAsyncListener(results, latch, exp));
      //prevSendPreRecoveryRequest = null;
      boolean success = latch.await(10, TimeUnit.SECONDS);
      if (!success) {
        if (prevSendPreRecoveryRequest != null) {
          prevSendPreRecoveryRequest.cancel();
          prevSendPreRecoveryRequest = null;
        }
        log.error("timeout waiting for prep recovery");
        return false;
      }
      Throwable exception = exp.get();
      if (exception != null) {
        log.error("failed in prep recovery", exception);
        return false;
      }

      NamedList result = results.get();
      log.info("results={}", result);
      String prepSuccess = result._getStr(CoreAdminHandler.RESPONSE_STATUS, null);
      if (prepSuccess != null) {
        return Integer.parseInt(prepSuccess) == 0;
      }
      return true;
    } catch (SolrException e) {
      Throwable cause = e.getRootCause();
      if (cause instanceof AlreadyClosedException) {
        close = true;
        throw new AlreadyClosedException(cause);
      }
      log.info("failed in prep recovery", e);
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    } catch (Exception e) {
      log.info("failed in prep recovery", e);
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    } 
      
  }

  private static class PrepRecoveryAsyncListener implements AsyncListener<NamedList<Object>> {
    private final AtomicReference<NamedList> results;
    private final CountDownLatch latch;
    private final AtomicReference<Throwable> exp;

    public PrepRecoveryAsyncListener(AtomicReference<NamedList> results, CountDownLatch latch, AtomicReference<Throwable> exp) {
      this.results = results;
      this.latch = latch;
      this.exp = exp;
    }

    @Override public void onSuccess(NamedList<Object> entries, int code) {
      results.set(entries);
      latch.countDown();
    }

    @Override public void onFailure(Throwable throwable, int code) {
      exp.set(throwable);
      latch.countDown();
    }
  }
}
