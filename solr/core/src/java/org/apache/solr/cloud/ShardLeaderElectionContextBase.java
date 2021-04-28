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

import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreDescriptor;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.OpResult.SetDataResult;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

class ShardLeaderElectionContextBase extends ElectionContext {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  protected final SolrZkClient zkClient;
  protected final LeaderElector leaderElector;
  protected volatile boolean closed;
  private final Integer id;
  private volatile boolean wasleader;

  public ShardLeaderElectionContextBase(LeaderElector leaderElector, String electionPath, String leaderPath, Replica replica, CoreDescriptor cd,
      SolrZkClient zkClient) {
    super(electionPath, leaderPath, replica, cd);
    this.zkClient = zkClient;
    this.id = replica.getInternalId();
    this.leaderElector = leaderElector;
  }

  @Override protected void cancelElection() throws InterruptedException, KeeperException {
    log.debug("cancel election for {}", replica);

    if (!zkClient.isAlive()) return;

    try {

      log.debug("Removing leader registration node on cancel");
      String leaderSeqPath = getLeaderSeqPath();
      if (leaderSeqPath != null) {
        zkClient.delete(leaderSeqPath, -1);
      }

      if (wasleader) {
        zkClient.delete(leaderPath + "/" + id, -1);
      }

    } catch (NoNodeException e) {

    } catch (Exception e) {
      log.info("Exception trying to cancel election {} {}", e.getClass().getName(), e.getMessage());
    }

    super.cancelElection();
  }

  @Override boolean runLeaderProcess(ElectionContext context, boolean weAreReplacement, int pauseBeforeStartMs)
      throws KeeperException, InterruptedException, IOException {
    wasleader = true;
    String leaderSeqPath = null;
    try {

      if (leaderElector.isLeader()) {
        return true;
      }

      leaderSeqPath = getLeaderSeqPath();

      if (leaderSeqPath == null) {
        log.warn("We have won as leader, but we have no leader election node known to us leaderPath " + leaderPath);
        return true;
      }

      log.debug("Creating leader registration node {} after winning as {}", leaderPath, leaderSeqPath);

      zkClient.mkdir(leaderPath + '/' + id, null, CreateMode.EPHEMERAL);

    } catch (NoNodeException e) {
      log.warn("No node exists for election " + leaderSeqPath + " " + leaderPath, e);
      return true;
    } catch (KeeperException.NodeExistsException e) {
      log.warn("Node already exists for election node=" + e.getPath(), e);

      return true;
    } catch (Exception e) {
      log.warn("Could not register as the leader because creating the ephemeral registration node in ZooKeeper failed", e);
      return false;
    }
    return true;
  }

  @Override public boolean isClosed() {
    return closed;
  }
}
