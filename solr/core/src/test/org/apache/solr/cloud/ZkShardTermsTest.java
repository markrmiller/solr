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

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrTestUtil;
import org.apache.solr.client.solrj.cloud.ShardTerms;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.util.TimeOut;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkShardTermsTest extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void beforeZkShardTermsTest() throws Exception {
    configureCluster(1)
        .addConfig("conf1", SolrTestUtil.TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();
  }

  @AfterClass
  public static void afterZkShardTermsTest() throws Exception {
    shutdownCluster();
  }

  @Test
  public void testRecoveringFlag() throws Exception {
    cluster.getZkClient().makePath("/collections/recoveringFlag/terms/s1", ZkStateReader.emptyJson, false);
    String collection = "recoveringFlag";
    try (ZkShardTerms zkShardTerms = new ZkShardTerms(collection, "s1", cluster.getZkClient())) {
      zkShardTerms.createWatcher();
      // List all possible orders of ensureTermIsHigher, startRecovering, doneRecovering
      zkShardTerms.registerTerm("replica1");
      zkShardTerms.registerTerm("replica2");

      // normal case when leader failed to send an update to replica
      zkShardTerms.ensureTermsIsHigher("replica1", Collections.singleton("replica2"));
      zkShardTerms.startRecovering("replica2");
      assertEquals(zkShardTerms.getTerm("replica2"), 1);
      assertEquals(zkShardTerms.getTerm("replica2_recovering"), 0);

      zkShardTerms.doneRecovering("replica2");
      assertEquals(zkShardTerms.getTerm("replica1"), 1);
      assertEquals(zkShardTerms.getTerm("replica2"), 1);
      assertEquals(zkShardTerms.getTerm("replica2_recovering"), -1);

      zkShardTerms.ensureTermsIsHigher("replica1", Collections.singleton("replica2"));
      assertEquals(zkShardTerms.getTerm("replica1"), 2);
      assertEquals(zkShardTerms.getTerm("replica2"), 1);
      assertEquals(zkShardTerms.getTerm("replica2_recovering"), -1);

      zkShardTerms.startRecovering("replica2");
      assertEquals(zkShardTerms.getTerm("replica2"), 2);
      assertEquals(zkShardTerms.getTerm("replica2_recovering"), 1);

      zkShardTerms.ensureTermsIsHigher("replica1", Collections.singleton("replica2"));
      assertEquals(zkShardTerms.getTerm("replica1"), 3);
      assertEquals(zkShardTerms.getTerm("replica2"), 2);
      assertEquals(zkShardTerms.getTerm("replica2_recovering"), 1);

      zkShardTerms.doneRecovering("replica2");
      assertEquals(zkShardTerms.getTerm("replica2"), 2);
      assertEquals(zkShardTerms.getTerm("replica2_recovering"), -1);

      zkShardTerms.startRecovering("replica2");
      zkShardTerms.doneRecovering("replica2");

      zkShardTerms.ensureTermsIsHigher("replica1", Collections.singleton("replica2"));
      zkShardTerms.startRecovering("replica2");
      zkShardTerms.ensureTermsIsHigher("replica1", Collections.singleton("replica2"));
      zkShardTerms.startRecovering("replica2");
      assertEquals(zkShardTerms.getTerm("replica1"), 5);
      assertEquals(zkShardTerms.getTerm("replica2"), 5);
      assertEquals(zkShardTerms.getTerm("replica2_recovering"), 3);
      zkShardTerms.doneRecovering("replica2");
      assertEquals(zkShardTerms.getTerm("replica2_recovering"), -1);

    }
  }

  @Test
  public void testCoreRemovalWhileRecovering() throws Exception {
    cluster.getZkClient().makePath("/collections/recoveringFlagRemoval/terms/s1", ZkStateReader.emptyJson, false);
    String collection = "recoveringFlagRemoval";
    try (ZkShardTerms zkShardTerms = new ZkShardTerms(collection, "s1", cluster.getZkClient())) {
      zkShardTerms.createWatcher();
      // List all possible orders of ensureTermIsHigher, startRecovering, doneRecovering
      zkShardTerms.registerTerm("replica1_rem");
      zkShardTerms.registerTerm("replica2_rem");

      // normal case when leader failed to send an update to replica
      zkShardTerms.ensureTermsIsHigher("replica1_rem", Collections.singleton("replica2_rem"));
      zkShardTerms.startRecovering("replica2_rem");
      assertEquals(zkShardTerms.getTerm("replica2_rem"), 1);
      assertEquals(zkShardTerms.getTerm("replica2_rem_recovering"), 0);

      // Remove core, and check if the correct core was removed as well as the recovering term for that core
      zkShardTerms.removeTerm("replica2_rem");
      assertEquals(zkShardTerms.getTerm("replica1_rem"), 1);
      assertEquals(zkShardTerms.getTerm("replica2_rem"), -1);
      assertEquals(zkShardTerms.getTerm("replica2_rem_recovering"), -1);
    }
  }

  @Ignore // MRM TODO: I violated this recently, need to see if that is the right thing to do
  public void testRegisterTerm() throws Exception {
    cluster.getZkClient().makePath("/collections/registerTerm/terms/s1", ZkStateReader.emptyJson, false);
    String collection = "registerTerm";
    ZkShardTerms rep1Terms = new ZkShardTerms(collection, "s1", cluster.getZkClient());
    rep1Terms.createWatcher();

    ZkShardTerms rep2Terms = new ZkShardTerms(collection, "s1", cluster.getZkClient());
    rep2Terms.createWatcher();

    rep1Terms.registerTerm("rep1");
    rep2Terms.registerTerm("rep2");
    try (ZkShardTerms zkShardTerms = new ZkShardTerms(collection, "s1", cluster.getZkClient())) {
      zkShardTerms.createWatcher();

      assertEquals(0L, zkShardTerms.getTerm("rep1"));
      assertEquals(0L, zkShardTerms.getTerm("rep2"));
    }
    waitFor(2, () -> rep1Terms.getTerms().size());
    rep1Terms.ensureTermsIsHigher("rep1", Collections.singleton("rep2"));
    assertEquals(1L, rep1Terms.getTerm("rep1"));
    assertEquals(0L, rep1Terms.getTerm("rep2"));

    // assert registerTerm does not override current value
    rep1Terms.registerTerm("rep1");
    assertEquals(1L, rep1Terms.getTerm("rep1"));

    waitFor(1L, () -> rep2Terms.getTerm("rep1"));
    rep2Terms.setTermEqualsToLeader("rep2");
    assertEquals(1L, rep2Terms.getTerm("rep2"));
    rep2Terms.registerTerm("rep2");
    assertEquals(1L, rep2Terms.getTerm("rep2"));

    // zkShardTerms must stay updated by watcher
    Map<String, Long> expectedTerms = new HashMap<>();
    expectedTerms.put("rep1", 1L);
    expectedTerms.put("rep2", 1L);

    TimeOut timeOut = new TimeOut(10, TimeUnit.SECONDS, new TimeSource.CurrentTimeSource());
    while (!timeOut.hasTimedOut()) {
      if (Objects.equals(expectedTerms, rep1Terms.getTerms()) && Objects.equals(expectedTerms, rep2Terms.getTerms())) break;
    }
    if (timeOut.hasTimedOut()) fail("Expected zkShardTerms must stay updated");

    rep1Terms.close();
    rep2Terms.close();
  }

  @Test
  @LuceneTestCase.Nightly
  public void testRaceConditionOnUpdates() throws Exception {
    cluster.getZkClient().makePath("/collections/raceConditionOnUpdates/terms/s1", ZkStateReader.emptyJson, false);
    String collection = "raceConditionOnUpdates";
    List<String> replicas = Arrays.asList("rep1", "rep2", "rep3", "rep4");
    for (String replica : replicas) {
      try (ZkShardTerms zkShardTerms = new ZkShardTerms(collection, "s1", cluster.getZkClient())) {
        zkShardTerms.createWatcher();
        zkShardTerms.registerTerm(replica);
      }
    }

    List<String> failedReplicas = new ArrayList<>(replicas);
    Collections.shuffle(failedReplicas, LuceneTestCase.random());
    while (failedReplicas.size() > 2) {
      failedReplicas.remove(0);
    }
    AtomicBoolean stop = new AtomicBoolean(false);
    Thread[] threads = new Thread[failedReplicas.size()];
    for (int i = 0; i < failedReplicas.size(); i++) {
      String replica = failedReplicas.get(i);
      threads[i] = new Thread(() -> {

        try (ZkShardTerms zkShardTerms = new ZkShardTerms(collection, "s1", cluster.getZkClient())) {
          zkShardTerms.createWatcher();

          while (!stop.get()) {
            try {
              Thread.sleep(LuceneTestCase.random().nextInt(TEST_NIGHTLY ? 200 : 50));
              zkShardTerms.setTermEqualsToLeader(replica);
            } catch (InterruptedException | KeeperException e) {
              ParWork.propagateInterrupt(e);
              log.error("", e);
            }
          }
        } catch (Exception e) {
          ParWork.propagateInterrupt(e);
          log.error("", e);
        }

      });
      threads[i].start();
    }

    long maxTerm = 0;
    try (ZkShardTerms shardTerms = new ZkShardTerms(collection, "s1", cluster.getZkClient())) {
      shardTerms.createWatcher();
      shardTerms.registerTerm("leader");
      TimeOut timeOut = new TimeOut(5, TimeUnit.SECONDS, new TimeSource.CurrentTimeSource());
      while (!timeOut.hasTimedOut()) {
        maxTerm++;
        assertEquals(shardTerms.getTerms().get("leader"), Collections.max(shardTerms.getTerms().values()));
        Thread.sleep(100);
      }
      assertTrue(maxTerm >= Collections.max(shardTerms.getTerms().values()));
    }
    stop.set(true);
    for (Thread thread : threads) {
      thread.join();
    }
  }

  public void testCoreTermWatcher() throws Exception {
    cluster.getZkClient().makePath("/collections/coreTermWatcher/terms/s1", ZkStateReader.emptyJson, false);
    String collection = "coreTermWatcher";
    ZkShardTerms leaderTerms = new ZkShardTerms(collection, "s1", cluster.getZkClient());
    leaderTerms.createWatcher();

    leaderTerms.registerTerm("leader");
    ZkShardTerms replicaTerms = new ZkShardTerms(collection, "s1", cluster.getZkClient());
    replicaTerms.createWatcher();

    AtomicInteger count = new AtomicInteger(0);
    // this will get called for almost 3 times
    ZkShardTerms.CoreTermWatcher watcher = new ZkShardTerms.CoreTermWatcher() {
      @Override
      public boolean onTermChanged(ShardTerms terms) {
        return count.incrementAndGet() < 3;
      }

      @Override
      public void close() {

      }
    };
    replicaTerms.addListener("replica", watcher);
    replicaTerms.registerTerm("replica");
    waitFor(1, count::get);
    leaderTerms.ensureTermsIsHigher("leader", Collections.singleton("replica"));
    waitFor(2, count::get);
    replicaTerms.setTermEqualsToLeader("replica");
    waitFor(3, count::get);

    leaderTerms.close();
    replicaTerms.close();

    waitFor(0, replicaTerms::getNumListeners);
  }

  public void testEnsureTermsIsHigher() {
    Map<String, Long> map = new HashMap<>();
    map.put("leader", 0L);
    ShardTerms terms = new ShardTerms(map, 0);
    terms = terms.increaseTerms("leader", Collections.singleton("replica"));
    assertEquals(1L, terms.getTerm("leader").longValue());
  }

  public void testSetTermToZero() throws Exception {
    cluster.getZkClient().makePath("/collections/setTermToZero/terms/s1", ZkStateReader.emptyJson, false);
    String collection = "setTermToZero";
    ZkShardTerms terms = new ZkShardTerms(collection, "s1", cluster.getZkClient());
    terms.createWatcher();

    terms.registerTerm("leader");
    terms.registerTerm("replica");
    terms.ensureTermsIsHigher("leader", Collections.singleton("replica"));
    assertEquals(1L, terms.getTerm("leader"));
    terms.setTermToZero("leader");
    assertEquals(0L, terms.getTerm("leader"));
    terms.close();
  }

  public void testReplicaCanBecomeLeader() throws Exception {
    cluster.getZkClient().makePath("/collections/replicaCanBecomeLeader/terms/s1", ZkStateReader.emptyJson, false);
    String collection = "replicaCanBecomeLeader";
    ZkShardTerms leaderTerms = new ZkShardTerms(collection, "s1", cluster.getZkClient());
    leaderTerms.createWatcher();

    ZkShardTerms replicaTerms = new ZkShardTerms(collection, "s1", cluster.getZkClient());
    replicaTerms.createWatcher();

    leaderTerms.registerTerm("leader");
    replicaTerms.registerTerm("replica");

    leaderTerms.ensureTermsIsHigher("leader", Collections.singleton("replica"));
    waitFor(false, () -> replicaTerms.canBecomeLeader("replica"));
    waitFor(true, () -> leaderTerms.skipSendingUpdatesTo("replica"));

    replicaTerms.startRecovering("replica");
    waitFor(false, () -> replicaTerms.canBecomeLeader("replica"));
    waitFor(false, () -> leaderTerms.skipSendingUpdatesTo("replica"));

    replicaTerms.doneRecovering("replica");
    waitFor(true, () -> replicaTerms.canBecomeLeader("replica"));
    waitFor(false, () -> leaderTerms.skipSendingUpdatesTo("replica"));

    leaderTerms.close();
    replicaTerms.close();
  }

  public void testSetTermEqualsToLeader() throws Exception {
    cluster.getZkClient().makePath("/collections/setTermEqualsToLeader/terms/s1", ZkStateReader.emptyJson, false);
    String collection = "setTermEqualsToLeader";
    ZkShardTerms leaderTerms = new ZkShardTerms(collection, "s1", cluster.getZkClient());
    leaderTerms.createWatcher();

    ZkShardTerms replicaTerms = new ZkShardTerms(collection, "s1", cluster.getZkClient());
    replicaTerms.createWatcher();

    leaderTerms.registerTerm("leader");
    replicaTerms.registerTerm("replica");

    leaderTerms.ensureTermsIsHigher("leader", Collections.singleton("replica"));
    waitFor(false, () -> replicaTerms.canBecomeLeader("replica"));
    waitFor(true, () -> leaderTerms.skipSendingUpdatesTo("replica"));

    replicaTerms.setTermEqualsToLeader("replica");
    waitFor(true, () -> replicaTerms.canBecomeLeader("replica"));
    waitFor(false, () -> leaderTerms.skipSendingUpdatesTo("replica"));

    leaderTerms.close();
    replicaTerms.close();
  }

  private <T> void waitFor(T expected, Supplier<T> supplier) throws InterruptedException {
    TimeOut timeOut = new TimeOut(10, TimeUnit.SECONDS, new TimeSource.CurrentTimeSource());
    while (!timeOut.hasTimedOut()) {
      if (expected == supplier.get()) return;
      Thread.sleep(10);
    }
    assertEquals(expected, supplier.get());
  }

}
