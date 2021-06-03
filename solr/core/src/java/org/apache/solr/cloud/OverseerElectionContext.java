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

import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;

final class OverseerElectionContext extends ShardLeaderElectionContextBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final Overseer overseer;
  private volatile boolean hasBeenOverseer;

  public OverseerElectionContext(LeaderElector leaderElector, final Integer zkNodeName, SolrZkClient zkClient, Overseer overseer) {
    super(leaderElector, Overseer.OVERSEER_ELECT, Overseer.OVERSEER_ELECT + "/leader", new Replica("overseer:" + overseer.getZkController().getNodeName(), getIDMap(zkNodeName, overseer),
        "overseer", -1, null), null, zkClient);
    this.overseer = overseer;
  }

  private static Object2ObjectMap<String,Object> getIDMap(Integer zkNodeName, Overseer overseer) {
    Object2ObjectMap<String,Object> idMap = new Object2ObjectLinkedOpenHashMap<>(2);
    idMap.put("id", zkNodeName);
    idMap.put(ZkStateReader.NODE_NAME_PROP, overseer.getZkController().getNodeName());
    return idMap;
  }

  @Override
  boolean runLeaderProcess(ElectionContext context, boolean weAreReplacement, int pauseBeforeStartMs) {
    log.info("Running the leader process for Overseer");

    if (overseer.isDone()) {
      log.info("Already closed, bailing ...");
      if (hasBeenOverseer) {
        cancelElection();
      }
      return true;
    }

    try {
      boolean success = super.runLeaderProcess(context, weAreReplacement, pauseBeforeStartMs);
      if (!success) {
        cancelElection();
        return false;
      }

    if (!overseer.getZkController().getCoreContainer().isShutDown() && !overseer.getZkController().isShutdownCalled()
        && !overseer.isDone()) {
      log.info("Starting overseer after winning Overseer election {}", replica.getInternalId());
      overseer.start(replica.getInternalId(), context, weAreReplacement);
    } else {
      log.info("Will not start Overseer because we are closed");
      cancelElection();
      return false;
    }

    hasBeenOverseer = true;
    return true;
    } catch (Exception e) {
      log.error("Exception becoming overseer", e);
      cancelElection();
      return false;
    }
  }

  public Overseer getOverseer() {
    return  overseer;
  }

  @Override
  public void cancelElection() {
    try {
      super.cancelElection();
    } catch (Exception e) {
      ParWork.propagateInterrupt(e);
      log.error("Exception closing Overseer", e);
    }
  }

  @Override
  public void joinedElectionFired() {

  }

  @Override
  public void checkIfIamLeaderFired() {

  }
}

