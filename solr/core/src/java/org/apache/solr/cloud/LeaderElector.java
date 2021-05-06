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

import com.codahale.metrics.Counter;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.common.util.metrics.Metrics;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Leader Election process. This class contains the logic by which a
 * leader is chosen. First call * {@link #setup(ElectionContext)} to ensure
 * the election process is init'd. Next call
 * {@link #joinElection(boolean)} to start the leader election.
 *
 * The implementation follows the classic ZooKeeper recipe of creating an
 * ephemeral, sequential node for each candidate and then looking at the set
 * of such nodes - if the created node is the lowest sequential node, the
 * candidate that created the node is the leader. If not, the candidate puts
 * a watch on the next lowest node it finds, and if that node goes down,
 * starts the whole process over by checking if it's the lowest sequential node, etc.
 *
 */
public class LeaderElector implements Closeable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final Counter leadersElected = Metrics.MARKS_METRICS.counter("leaderelector_leaderselected");

  private static final Counter leadersFailed = Metrics.MARKS_METRICS.counter("leaderelector_leadersfailed");

  private static final Counter leadersCheckedIfIamLeader = Metrics.MARKS_METRICS.counter("leaderelector_checkifiamleader");

  public static final String ELECTION_NODE = "/election";

  public final static Pattern LEADER_SEQ = Pattern.compile(".*?/?.*?-n_(\\d+)");
  private final static Pattern SESSION_ID = Pattern.compile(".*?/?(.*?-.*?)-n_\\d+");

  private final ActionThrottle leaderThrottle = new ActionThrottle("leader", Integer.getInteger("solr.leaderThrottle", 0));

  public enum State {
    LEADER, ELECTION, IDLE
  }

  protected final SolrZkClient zkClient;
  private final ZkController zkController;

  private volatile ElectionContext context;

  private volatile ElectionWatcher watcher;

  private volatile boolean isClosed;

  private volatile boolean disableRemoveWatches = false;

  private final ExecutorService executor;

  private volatile State state = State.IDLE;

  //  public LeaderElector(SolrZkClient zkClient) {
  //    this.zkClient = zkClient;
  //    this.contextKey = null;
  //    this.electionContexts = new ConcurrentHashMap<>(132, 0.75f, 50);
  //  }

  public LeaderElector(ZkController zkController) {

    this.zkClient = zkController.getZkClient();
    this.zkController = zkController;
    executor = new ThreadPoolExecutor(0, 1,
            0L, TimeUnit.MILLISECONDS,
            new SynchronousQueue<>(), new SolrNamedThreadFactory("LeaderElector", true));
    assert ObjectReleaseTracker.getInstance().track(this);
  }

  public ElectionContext getContext() {
    return context;
  }

  /**
   * Check if the candidate with the given n_* sequence number is the leader.
   * If it is, set the leaderId on the leader zk node. If it is not, start
   * watching the candidate that is in line before this one - if it goes down, check
   * if this candidate is the leader again.
   *
   * @param replacement has someone else been the leader already?
   */
  private boolean checkIfIamLeader(final ElectionContext context, boolean replacement) throws InterruptedException {
    try {
      MDCLoggingContext.setCoreName(context.replica.getName());
      leadersCheckedIfIamLeader.inc();

      if (log.isDebugEnabled()) log.debug("{} Check if I am leader {}",  context.replica.getSlice(), context.getClass().getSimpleName());

      leaderThrottle.minimumWaitBetweenActions();
      leaderThrottle.markAttemptingAction();

      // get all other numbers...
      final String holdElectionPath = context.electionPath + ELECTION_NODE;
      List<String> seqs;

      try {
        seqs = zkClient.getChildren(holdElectionPath, null, true);
      } catch (KeeperException.NoNodeException e) {
        log.info("No election node found {}", holdElectionPath);
        return false;
      }

      sortSeqs(seqs);

      String leaderSeqNodeName;

      leaderSeqNodeName = context.getLeaderSeqPath().substring(context.getLeaderSeqPath().lastIndexOf('/') + 1);

      if (!seqs.contains(leaderSeqNodeName)) {
        log.debug("{} Our node is no longer in line to be leader", context.replica.getSlice());

        return false;
      }

      if (log.isDebugEnabled()) {
        log.debug("{} The leader election node is {}", context.replica.getSlice(), leaderSeqNodeName);
      }

      if (leaderSeqNodeName.equals(seqs.get(0))) {

        // I am the leader
        if (log.isDebugEnabled()) log.debug("{} I am the potential leader {}, running leader process",  context.replica.getSlice(), context.replica.getName());

        //          if ((zkController != null && zkController.getCoreContainer().isShutDown())) {
        //            if (log.isDebugEnabled()) log.debug("Elector is closed, will not try and run leader processes");
        //            state = OUT_OF_ELECTION;
        //            return false;
        //          }

        boolean success = runIamLeaderProcess(context, replacement);
        log.debug("{} result of trying to become leader for {} is {}",  context.replica.getSlice(),  context.replica.getName(), success);
        return !success;
      }

      String toWatch = seqs.get(0);
      for (String node : seqs) {
        if (leaderSeqNodeName.equals(node)) {
          break;
        }
        toWatch = node;
      }

      String watchedNode = holdElectionPath + "/" + toWatch;

      log.debug("{} I am not the leader (our path is ={}) - watch the node below me {} seqs={}", context.replica.getSlice(), leaderSeqNodeName, watchedNode, seqs);

      watcher = new ElectionWatcher(context.getLeaderSeqPath(), watchedNode, context);
      Stat exists = zkClient.exists(watchedNode, watcher);

      if (log.isDebugEnabled()) log.debug("Watching path {} to know if I could be the leader, my node is {}", watchedNode, context.getLeaderSeqPath());

      return false;
    } catch (AlreadyClosedException e) {
      log.debug("{} already closed", context.replica.getSlice(), e);
      return false;
    } catch (KeeperException.SessionExpiredException e) {
      log.warn("{} session expired", context.replica.getSlice(), e);
      return false;
    } catch (Exception e) {
      log.error("{} failed in checkIfIamLeader", context.replica.getSlice(), e);
      return true;

    } finally {
      MDCLoggingContext.clear();
    }
  }

  protected boolean runIamLeaderProcess(final ElectionContext context, boolean weAreReplacement) throws KeeperException, InterruptedException, IOException {
    boolean success = false;
    try {
      success = context.runLeaderProcess(context, weAreReplacement, 0);
      if (success) {
        state = State.LEADER;
        leadersElected.inc();
        return true;
      }
      log.warn("{} Failed becoming leader {}", context.replica, context.replica);
      return false;
    } finally {
      if (!success) {
        leadersFailed.inc();
      }
    }
  }

  /**
   * Returns int given String of form n_0000000001 or n_0000000003, etc.
   *
   * @return sequence number
   */
  public static int getSeq(String nStringSequence) {
    int seq;
    Matcher m = LEADER_SEQ.matcher(nStringSequence);
    if (m.matches()) {
      seq = Integer.parseInt(m.group(1));
    } else {
      throw new IllegalStateException("Could not find regex match in:" + nStringSequence);
    }
    return seq;
  }

  private static String getNodeId(String nStringSequence) {
    String id;
    Matcher m = SESSION_ID.matcher(nStringSequence);
    if (m.matches()) {
      id = m.group(1);
    } else {
      throw new IllegalStateException("Could not find regex match in:" + nStringSequence);
    }
    return id;
  }

  public static String getNodeName(String nStringSequence) {
    return nStringSequence;
  }

  public void joinElection(boolean replacement) {
    joinElection(replacement, false);
  }

  public void joinElection(boolean replacement, boolean joinAtHead) {
    if (!zkController.getCoreContainer().isShutDown() && !zkController.isDcCalled()) {
      isClosed = false;
      executor.submit(() -> {
        MDCLoggingContext.setCoreName(context.replica.getName());
        MDCLoggingContext.setNode(zkController.getNodeName());
        try {
          doJoinElection(context, replacement, joinAtHead);
        } catch (Exception e) {
          log.error("Exception trying to join election", e);
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
        } finally {
          MDCLoggingContext.clear();
        }
      });
    } else {
      throw new AlreadyClosedException();
    }
  }

  /**
   * Begin participating in the election process. Gets a new sequential number
   * and begins watching the node with the sequence number before it, unless it
   * is the lowest number, in which case, initiates the leader process. If the
   * node that is watched goes down, check if we are the new lowest node, else
   * watch the next lowest numbered node.
   */
  public void doJoinElection(ElectionContext context, boolean replacement, boolean joinAtHead) throws KeeperException, InterruptedException {
    log.debug("{} joining leader election replica={}", context.replica.getSlice(), context.replica.getName());
    if (shouldRejectJoins()) {
      log.info("{} Won't join election {}", context.replica.getSlice(), state);
      throw new AlreadyClosedException();
    }

    final String shardsElectZkPath = context.electionPath + LeaderElector.ELECTION_NODE;

    long sessionId;
    String id = null;
    String leaderSeqPath;

    while (true) {
      try {
        sessionId = zkClient.getSessionId();
        id = sessionId + "-" + context.replica.getInternalId();
        if (joinAtHead) {
          if (log.isDebugEnabled()) log.debug("Node {} trying to join election at the head", id);
          List<String> nodes = OverseerTaskProcessor.getSortedElectionNodes(zkClient, shardsElectZkPath);
          if (nodes.size() < 2) {
            leaderSeqPath = zkClient.create(shardsElectZkPath + "/" + id + "-n_", (byte[]) null, CreateMode.EPHEMERAL_SEQUENTIAL, true);
          } else {
            String firstInLine = nodes.get(1);
            if (log.isDebugEnabled()) log.debug("The current head: {}", firstInLine);
            Matcher m = LEADER_SEQ.matcher(firstInLine);
            if (!m.matches()) {
              String msg = "Could not find regex match in:" + firstInLine;
              log.error("Could not find regex match in:{}", firstInLine);
              throw new IllegalStateException(msg);
            }
            leaderSeqPath = shardsElectZkPath + "/" + id + "-n_" + m.group(1);
            zkClient.create(leaderSeqPath, (byte[]) null, CreateMode.EPHEMERAL, false);
          }
        } else {
          if (log.isDebugEnabled()) {
            log.debug("create ephem election node {}", shardsElectZkPath + '/' + id + "-n_");
          }
          leaderSeqPath = zkClient.create(shardsElectZkPath + "/" + id + "-n_", (byte[]) null, CreateMode.EPHEMERAL_SEQUENTIAL, false);
        }

        ParWork.submitIO("joinedElectionFired", context::joinedElectionFired);
        log.debug("{} Joined leadership election with path: {}", context.replica.getSlice(), leaderSeqPath);
        context.setLeaderSeqPath(leaderSeqPath);

        break;
      } catch (ConnectionLossException e) {
        log.info("{} ConnectionLoss during leader election, will wait until reconnected ...", context.replica.getSlice());
        if (zkClient.getConnectionManager().getKeeper().getState() == ZooKeeper.States.CLOSED) {
          log.info("{} Won't retry to create election node on ConnectionLoss because the client state is closed", context.replica.getSlice());
          break;
        }

        zkClient.getConnectionManager().waitForConnected();

        log.info("{} Reconnected after ConnectionLoss, checking if we made our election node", context.replica.getSlice());
        // we don't know if we made our node or not...
        List<String> entries = zkClient.getChildren(shardsElectZkPath, null, null, true, true);

        for (String entry : entries) {
          String nodeId = getNodeId(entry);
          if (id.equals(nodeId)) {
            // we did create our node...
            break;
          }
        }

      } catch (KeeperException.NoNodeException e) {
        log.error("{} No leader election node found", context.replica.getSlice(), e);
        break;
      }
    }

    log.debug("{} Do checkIfIamLeader for {}", context.replica.getSlice(),  context.replica.getName());
    boolean tryagain = true;

    while (tryagain) {
      tryagain = checkIfIamLeader(context, replacement);
    }

  }

  private boolean shouldRejectJoins() {
    return zkController.getCoreContainer().isShutDown() || zkController.isDcCalled();
  }

  @Override public void close() throws IOException {
    assert ObjectReleaseTracker.getInstance().release(this);
    state = State.IDLE;
    this.isClosed = true;

    IOUtils.closeQuietly(watcher);
    watcher = null;

    if (context != null) {
      try {
        context.cancelElection();
      } catch (Exception e) {
        log.warn("Exception canceling election", e);
      }
    }
  }

  public boolean isClosed() {
    return isClosed;
  }

  public State getState() {
    return state;
  }

  private class ElectionWatcher implements Watcher, Closeable {
    final String myNode, watchedNode;
    final ElectionContext context;
    private volatile boolean closed;

    private ElectionWatcher(String myNode, String watchedNode, ElectionContext context) {
      this.myNode = myNode;
      this.watchedNode = watchedNode;
      this.context = context;
    }

    @Override public void process(WatchedEvent event) {
      // session events are not change events, and do not remove the watcher
      if (EventType.None.equals(event.getType()) || closed) {
        return;
      }

      if (log.isDebugEnabled()) log.debug("Got event on node we where watching in leader line {} watchedNode={}", myNode, watchedNode);

      if (state == State.LEADER) {
        log.warn("Election watcher fired, but we are already leader");
      }

      executor.submit(() -> {
        try {
          if (event.getType() == EventType.NodeDeleted) {
            // am I the next leader?
            state = State.ELECTION;
            boolean tryagain = true;
            while (tryagain) {
              tryagain = checkIfIamLeader(context, true);
            }
          } else {

//            Stat exists = zkClient.exists(watchedNode, this);
//            if (exists == null) {
//              close();
//              boolean tryagain = true;
//
//              while (tryagain) {
//                tryagain = checkIfIamLeader(context, true);
//              }
//            }

          }
          // we don't kick off recovery here, the leader sync will do that if necessary for its replicas
        } catch (AlreadyClosedException | InterruptedException e) {
          log.info("Already shutting down");
        } catch (Exception e) {
          log.error("Exception in election", e);
        }
      });

    }

    @Override public void close() throws IOException {
      this.closed = true;
      if (disableRemoveWatches) {
        return;
      }
      try {
        zkClient.removeWatches(watchedNode, this, WatcherType.Any, true);
      } catch (KeeperException.NoWatcherException | AlreadyClosedException e) {

      } catch (Exception e) {
        log.info("could not remove watch {} {}", e.getClass().getSimpleName(), e.getMessage());
      }
    }
  }

  public void disableRemoveWatches() {
    this.disableRemoveWatches = true;
  }

  /**
   * Set up any ZooKeeper nodes needed for leader election.
   */
  public void setup(final ElectionContext context) {
    this.context = context;
  }

  /**
   * Sort n string sequence list.
   */
  public static void sortSeqs(List<String> seqs) {
    seqs.sort(Comparator.comparingInt(LeaderElector::getSeq).thenComparing(o -> o));
  }

  synchronized void retryElection(boolean joinAtHead) {
    if (shouldRejectJoins()) {
      log.info("Closed, won't rejoin election");
      throw new AlreadyClosedException();
    }

    IOUtils.closeQuietly(this);
    if (context instanceof ShardLeaderElectionContextBase) {
      ((ShardLeaderElectionContextBase) context).closed = false;
    }

    isClosed = false;
    joinElection(true, joinAtHead);
  }

  public boolean isLeader() {
    return State.LEADER == state;
  }
}
