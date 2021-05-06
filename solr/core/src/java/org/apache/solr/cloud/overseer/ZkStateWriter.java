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
package org.apache.solr.cloud.overseer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;

import com.codahale.metrics.Meter;
import org.apache.solr.cloud.ActionThrottle;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.Stats;
import org.apache.solr.cloud.api.collections.Assign;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.common.util.Utils;
import org.apache.solr.common.util.metrics.Metrics;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.singletonMap;


// TODO: live node listener to clear states
public class ZkStateWriter {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final Meter stateUpdateWrites = Metrics.MARKS_METRICS.meter("zkstatewriter_stateupdates");
  private static final Meter structureWrites = Metrics.MARKS_METRICS.meter("zkstatewriter_structureupdates");

  private final ZkStateReader reader;
  private final Overseer overseer;

  /**
   * Represents a no-op {@link ZkWriteCommand} which will result in no modification to cluster state
   */

  protected volatile Stats stats;

  private final Map<Integer, Map<Integer,Integer>> stateUpdates = new ConcurrentHashMap<>(64);

//  Map<Long,List<ZkStateWriter.StateUpdate>> sliceStates = new ConcurrentHashMap<>();

  private final Map<Integer,String> idToCollection = new ConcurrentHashMap<>(64);

  private final Map<String,DocAssign> assignMap = new ConcurrentHashMap<>(64);

  private final Map<String,ReentrantLock> collLocks = new ConcurrentHashMap<>(64);

  private final Map<String,ActionThrottle> stateWriteThrottles = new ConcurrentHashMap<>(64);

  private final Map<String,DocCollection> cs = new ConcurrentHashMap<>(64);


  private final AtomicInteger ID = new AtomicInteger();

  private final Set<String> dirtyStructure = ConcurrentHashMap.newKeySet();

  private volatile ExecutorService workerExec;
  private volatile long start;

  public ZkStateWriter(ZkStateReader zkStateReader, Stats stats, Overseer overseer) {
    this.overseer = overseer;
    this.reader = zkStateReader;
    this.stats = stats;

  }

  public void enqueueStateUpdates(Map<Integer,Map<Integer,Integer>> replicaStates,  Map<Integer,List<ZkStateWriter.StateUpdate>> sliceStates) {
    log.debug("enqueue state updates");

    replicaStates.forEach((collectionId, idToStateMap) -> {
      // log.debug("enqueue state updates for {} {}", replicaStatesEntry.getKey(), replicaStatesEntry.getValue());
      // this.stateUpdates.compute(replicaStatesEntry.getKey(), aLong -> replicaStatesEntry.getValue());
      this.stateUpdates.compute(collectionId, (id, map) -> {

        if (map == null) {
          Map<Integer,Integer> newMap = new ConcurrentHashMap<>(idToStateMap.size());
          newMap.putAll(idToStateMap);
          return newMap;
        }
        idToStateMap.forEach((integer, newState) -> {
          if (map.containsKey(integer)) {
            map.replace(integer, newState);
          } else {
            map.put(integer, newState);
          }
        });

        return map;
      });

      // log.debug("enqueue state updates result {} {}", replicaStatesEntry.getKey(), stateUpdates.get(replicaStatesEntry.getKey()));
    });

    sliceStates.forEach((collectionId, stateUpdates) -> {
      String collection = idToCollection.get(collectionId);

      DocCollection docColl = cs.get(collection);

      boolean didUpdate = false;

      for (StateUpdate update : stateUpdates) {
        Slice slice = docColl.getSlice(update.sliceName);
        if (slice != null) {
          didUpdate = true;
          slice.setState(update.state);
        }
      }

      if (didUpdate) {
        dirtyStructure.add(collection);
        writeStructureUpdates(collection);
      }
    });
  }

  public void enqueueStructureChange(DocCollection docCollection) {

    try {

      log.debug("enqueue structure change docCollection={} replicas={}", docCollection, docCollection.getReplicas());

      String collectionName = docCollection.getName();

      idToCollection.put(docCollection.getId(), docCollection.getName());

      ReentrantLock collLock = collLocks.compute(collectionName, (s, reentrantLock) -> {
        if (reentrantLock == null) {
          return new ReentrantLock();
        }
        return reentrantLock;
      });
      collLock.lock();
      try {

        docCollection = docCollection.copy();

        DocCollection currentCollection = cs.get(docCollection.getName());
        log.trace("zkwriter collection={}", docCollection);
        log.trace("zkwriter currentCollection={}", currentCollection);
        dirtyStructure.add(docCollection.getName());


        if (currentCollection != null) {
          List<String> removeSlices = new ArrayList<>();
          List<String> removeReplicas = new ArrayList<>();

          Map<String,Slice> newSlices = currentCollection.getSlicesCopy();
          for (Slice docCollectionSlice : docCollection.getSlices()) {
            if (docCollectionSlice.get("remove") != null) {
              removeSlices.add(docCollectionSlice.getName());
              continue;
            }

            Map<String,Replica> newReplicaMap = docCollectionSlice.getReplicasCopy();
            for (Replica replica : docCollectionSlice.getReplicas()) {
              if (replica.get("remove") != null) {
                removeReplicas.add(replica.getName());
                continue;

              }

              Map newProps = new HashMap(2);

              Replica newReplica = replica.copyWithProps(newProps);
              newReplicaMap.put(replica.getName(), newReplica);

              for (String removeReplica : removeReplicas) {
                newReplicaMap.remove(removeReplica);
              }

              Slice newDocCollectionSlice = docCollectionSlice.copyWithReplicas(newReplicaMap);

              newSlices.put(newDocCollectionSlice.getName(), newDocCollectionSlice);

            }
            for (String removeSlice : removeSlices) {
              newSlices.remove(removeSlice);
            }
          }
          Map newDocProps = new HashMap(currentCollection.getProps());
          newDocProps.forEach(newDocProps::put);
          newDocProps.remove("pullReplicas");
          newDocProps.remove("replicationFactor");
          newDocProps.remove("maxShardsPerNode");
          newDocProps.remove("nrtReplicas");
          newDocProps.remove("tlogReplicas");
          newDocProps.remove("numShards");
          DocCollection newCollection = currentCollection.copyWithSlices(newSlices, newDocProps);
          log.debug("zkwriter newCollection={} replicas={}", newCollection, newCollection.getReplicas());
          cs.put(currentCollection.getName(), newCollection);

        } else {
          Map<String,Object> newDocProps = new HashMap<>(docCollection.getProps());

          Map<String,Slice> newSlices = docCollection.getSlicesCopy();
          List<String> removeSlices = new ArrayList<>();
          for (Slice slice : docCollection) {

            if (slice.get("remove") != null) {
              removeSlices.add(slice.getName());
            }

            for (Replica replica : slice.getReplicas()) {

              Map<String,Replica> newReplicaMap = slice.getReplicasCopy();
              Map<Object,Object> newProps = new HashMap<>(2);

              Replica newReplica = replica.copyWithProps(newProps);
              newReplicaMap.put(newReplica.getName(), newReplica);
              Slice newSlice = slice.copyWithReplicas(newReplicaMap);
              newSlices.put(newSlice.getName(), newSlice);
            }

          }
          for (String removeSlice : removeSlices) {
            newSlices.remove(removeSlice);
          }

          newDocProps.remove("pullReplicas");
          newDocProps.remove("replicationFactor");
          newDocProps.remove("maxShardsPerNode");
          newDocProps.remove("nrtReplicas");
          newDocProps.remove("tlogReplicas");
          newDocProps.remove("numShards");
          cs.put(docCollection.getName(), docCollection.copyWithSlices(newSlices, newDocProps));
        }

      } finally {
        collLock.unlock();
      }

    } catch (Exception e) {
      log.error("Exception while queuing update", e);
      throw e;
    }
  }

  public Integer lastWrittenVersion(String collection) {
    DocCollection col = cs.get(collection);
    if (col == null) {
      return 0;
    }
    return col.getZNodeVersion();
  }

  /**
   * Writes all pending updates to ZooKeeper and returns the modified cluster state
   *
   */

  public Future writeStructureUpdates(String collection) {
    return ParWork.submit("zkStateWriter#writePendingUpdates", () -> {
      MDCLoggingContext.setNode(overseer.getCoreContainer().getZkController().getNodeName());

      do {
        try {
          write(collection);
          break;
        } catch (KeeperException.BadVersionException e) {
          log.warn("hit bad version trying to write state.json, trying again ...");
        } catch (Exception e) {
          log.error("write pending failed", e);
          break;
        }
        final boolean closed = overseer.getZkStateReader().getZkClient().isClosed();
        final boolean overseerClosed = overseer.isClosed();
        if (closed || overseerClosed) {
          break;
        }
      } while (true);
    });
  }

  private void write(String coll) throws KeeperException.BadVersionException {

    if (log.isDebugEnabled()) {
      log.debug("writePendingUpdates {}", coll);
    }

    log.debug("process collection {}", coll);
//    ReentrantLock collLock = collLocks.compute(coll, (s, reentrantLock) -> {
//      if (reentrantLock == null) {
//        return new ReentrantLock();
//      }
//      return reentrantLock;
//    });
//    collLock.lock();
    try {

      DocCollection collection = cs.get(coll);

      if (collection == null) {
        return;
      }


      if (log.isTraceEnabled()) log.trace("check collection {} {}", collection, dirtyStructure);

      //  collState.throttle.minimumWaitBetweenActions();
      //  collState.throttle.markAttemptingAction();
      String name = collection.getName();
      String path = ZkStateReader.getCollectionPath(collection.getName());
      String pathSCN = ZkStateReader.getCollectionSCNPath(collection.getName());

      if (log.isTraceEnabled()) log.trace("process {}", collection);
      try {

        if (dirtyStructure.contains(name)) {
          if (log.isDebugEnabled()) log.debug("structure change in {}", collection.getName());

          byte[] data = Utils.toJSON(singletonMap(name, collection));

          if (log.isDebugEnabled()) log.debug("Write state.json prevVersion={} bytes={} col={} ", collection.getZNodeVersion(), data.length, collection);

          if (reader == null) {
            log.error("read not initialized in zkstatewriter");
            return;
          }
          if (reader.getZkClient() == null) {
            log.error("zkclient not initialized in zkstatewriter");
            return;
          }

          dirtyStructure.remove(collection.getName());

          structureWrites.mark();
          reader.getZkClient().setData(path, data, -1, (rc, path1, ctx, stateJsonStat) -> {
            if (rc != 0) {
              KeeperException e = KeeperException.create(KeeperException.Code.get(rc), path1);
              log.error("Exception on trigger scn znode path={}", path1, e);

              if (e instanceof KeeperException.BadVersionException) {

                log.info("Tried to update state.json for {} with bad version {} \n {}", coll, -1, collection);

              }
              if (e instanceof KeeperException.ConnectionLossException) {
                dirtyStructure.add(coll);
                overseer.getZkStateWriter().writeStructureUpdates(coll);
              }

            } else {
              try {
                reader.getZkClient().setData(pathSCN, null, -1, (rc2, path2, ctx2, scnStat) -> {
                  if (rc2 != 0) {
                    KeeperException e = KeeperException.create(KeeperException.Code.get(rc2), path2);
                    log.error("Exception on trigger scn znode path={}", path2, e);
                  }
                }, "pathSCN");
              } catch (Exception e) {
                log.error("Exception triggering SCN node");
              }
            }
          }, "state.json");

        }

      } catch (Exception e) {
        log.error("Failed processing update={}", collection, e);
      }

    } finally {
    //  collLock.unlock();
    }

  }

  public ClusterState getClusterstate(String collection) {

    Map<String,DocCollection> map;
    if (collection != null) {

      map = new HashMap<>(1);
      DocCollection coll = cs.get(collection);
      if (coll != null) {
        map.put(collection, coll.copy());
      }

    } else {
      map = new HashMap<>(cs.keySet().size());
      cs.forEach((s, docCollection) -> map.put(s, docCollection.copy()));
    }

    return ClusterState.getRefCS(map, -2);

  }

//  public String getCollectionForId(Integer id) {
//    AtomicReference<String> collectionName = new AtomicReference<>();
//
//    for (DocCollection docCollection : cs.values()) {
//      if (docCollection.getId().equals(id)) {
//        collectionName.set(docCollection.getName());
//        break;
//      }
//    }
//
//    String name = collectionName.get();
//    if (name == null) {
//      Collection<DocCollection> watchedCollectionStates = reader.getClusterState().getWatchedCollectionStates();
//
//      for (DocCollection docCollection : watchedCollectionStates) {
//        if (docCollection.getId().equals(id)) {
//          collectionName.set(docCollection.getName());
//          break;
//        }
//      }
//    }
//
//    name = collectionName.get();
//    if (name == null) {
//      Collection<ClusterState.CollectionRef> lazydCollectionStates = reader.getClusterState().getLazyCollectionStates();
//      for (ClusterState.CollectionRef docCollection : lazydCollectionStates) {
//        DocCollection docColl = docCollection.get().join();
//        if (docColl != null) {
//          if (docColl.getId().equals(id)) {
//            collectionName.set(docColl.getName());
//            break;
//          }
//        }
//      }
//    }
//
//    return collectionName.get();
//  }

  public Set<String> getDirtyStructureCollections() {
    return dirtyStructure;
  }


  public void removeCollection(String collection) {
    log.debug("Removing collection from zk state {}", collection);
    try {
      ReentrantLock collLock = collLocks.compute(collection, (s, reentrantLock) -> {
        if (reentrantLock == null) {
          return new ReentrantLock();
        }
        return reentrantLock;
      });
      collLock.lock();
      try {


        assignMap.remove(collection);
        dirtyStructure.remove(collection);

        DocCollection removed = cs.remove(collection);
        if (removed != null) {
          stateUpdates.remove(removed.getId());
          idToCollection.remove(removed.getId());
          List<Replica> replicas = removed.getReplicas();
          for (Replica replica : replicas) {
            overseer.getCoreContainer().getZkController().clearCachedState(replica.getName());
          }

        }

      } finally {
        collLock.unlock();
        collLocks.compute(collection, (s, reentrantLock) -> null);
      }
    } catch (Exception e) {
      log.error("Exception removing collection", e);

    }
  }

  public Integer getHighestId(String collection) {
    Integer id = ID.incrementAndGet();
    idToCollection.put(id, collection);
    return id;
  }

  public int getReplicaAssignCnt(String collection, String shard, String namePrefix) {

    DocAssign docAssign = assignMap.computeIfAbsent(collection, c -> new DocAssign());

    int id = docAssign.replicaAssignCnt.incrementAndGet();
    log.debug("assign id={} for collection={} slice={} namePrefix={}", id, collection, shard, namePrefix);
    return id;
  }

  public void init(boolean weAreReplacement) {
    log.info("ZkStateWriter Init - A new Overseer in charge or we are back baby replacement={}", weAreReplacement);
    start = System.nanoTime();
    try {

      overseer.getCoreContainer().getZkController().clearStatePublisher();

      Worker worker = new Worker();
      workerExec = Executors.newSingleThreadExecutor(new SolrNamedThreadFactory("ZKStateWriter", true));
      workerExec.submit(worker);

      int[] highId = new int[1];
      Map<String,ClusterState.CollectionRef> collectionRefs = reader.getCollectionRefs();

      collectionRefs.forEach((collectionName, docStateRef) -> {
        DocCollection docState = docStateRef.get(false).join();
        idToCollection.put(docState.getId(), collectionName);
        if (weAreReplacement) {
          stateUpdates.compute(docState.getId(), (k, v) -> {
            //
//                        if (v != null) {
//                          return v;
//                        }

            String stateUpdatesPath = ZkStateReader.getCollectionStateUpdatesPath(collectionName);
            byte[] data;
            Stat stat2 = new Stat();
            try {
              data = reader.getZkClient().getData(stateUpdatesPath, null, stat2, true, false);
            } catch (Exception e) {
              log.info("No node found for {}", stateUpdatesPath);
              return v;
            }

            Map<Integer,Integer> m;

            if (data == null) {
              log.debug("No data found for {}", stateUpdatesPath);
              return v;
            }

            try {
              m = Collections.unmodifiableMap(new LinkedHashMap<>((Map) Utils.fromJavabin(data)));
            } catch (IOException e) {
              log.error("Error parsing state updates", e);
              return v;
            }
            if (v == null) {
              v = new ConcurrentHashMap<>();
            }

            Map<Integer,Integer> finalV = v;
            m.forEach((id, s) -> {
              //if (!finalV.containsKey(id)) {
                finalV.put(id, s);
              //}
            });

            return finalV;
          });

          writeStateUpdatesInternal(Collections.singleton(docState.getId()), 1);
        } else {
//          Map<Integer,Integer> su = stateUpdates.get(docState.getId());
//          if (su != null) {
//            su.clear();
//            writeStateUpdatesInternal(Collections.singleton(docState.getId()), 1);
//          }
        }

        cs.put(collectionName, docState);

        if (docState.getId() > highId[0]) {
          highId[0] = docState.getId();
        }

        DocAssign docAssign = new DocAssign();
        assignMap.put(collectionName, docAssign);
        int max = 1;
        Collection<Slice> slices = docState.getSlices();
        for (Slice slice : slices) {
          Collection<Replica> replicas = slice.getReplicas();

          for (Replica replica : replicas) {
            Matcher matcher = Assign.pattern.matcher(replica.getName());
            if (matcher.matches()) {
              int val = Integer.parseInt(matcher.group(1));
              max = Math.max(max, val);
            }
          }
        }
        docAssign.replicaAssignCnt.set(max);

      });

      ID.set(highId[0]);

      reader.registerLiveNodesListener((oldLiveNodes, newLiveNodes) -> {

        Set<String> lostLiveNodes = new HashSet<>(oldLiveNodes);
        lostLiveNodes.removeAll(newLiveNodes);
        log.info("Detected nodes that went down, removing states for nodes=[{}]...", lostLiveNodes);
        if (lostLiveNodes.size() == 0) {
          return false;
        }
        Set<Integer> collIds = new HashSet<>();
        Set<Map.Entry<String,DocCollection>> entrySet = cs.entrySet();
        for (Map.Entry<String,DocCollection> entry : entrySet) {
          DocCollection coll = entry.getValue();

          stateUpdates.compute(coll.getId(), (integer, integerIntegerMap) -> {
            if (integerIntegerMap == null) {
              return null;
            }
            boolean write = false;
            for (String node : lostLiveNodes) {
              List<Replica> replicas = coll.getReplicas(node);
              for (Replica replica : replicas) {
                collIds.add(coll.getId());
                if (log.isDebugEnabled()) {
                  log.debug("Set an inactive state for replica {} on node {} ...", replica, replica.getNodeName());
                }
                integerIntegerMap.remove(replica.getInternalId());
                write = true;
              }
            }
            if (write) {
              writeStateUpdatesInternal(collIds, 1);
            }
            return integerIntegerMap;
          });
        }

        return false;
      });

      if (log.isDebugEnabled()) log.debug("zkStateWriter starting with cs {}", cs);
    } catch (Exception e) {
      log.error("Exception in ZkStateWriter init", e);
    }
  }

  public Set<String> getCollections() {
    return cs.keySet();
  }

  public void writeStateUpdates(Set<Integer> collIds) throws InterruptedException {
    Set<Integer> workSet = ConcurrentHashMap.newKeySet(collIds.size());
    workSet.addAll(collIds);
    workQueue.put(workSet);
//    Set<CompletableFuture> futures = new HashSet<>(collIds.size());
//    for (Integer collId : collIds) {
//      CompletableFuture<Object> future = new CompletableFuture<>();
//      futures.add(future);
//    }
//    writeStateUpdatesInternal(collIds, futures);
//    return futures;
  }

  private static final byte VERSION = 2;

  private final static Set<Integer> TERMINATED = new HashSet<>(0);

  public void stop() throws InterruptedException {


    //if (!workQueue.tryTransfer(TERMINATED)) {
    workQueue.put(TERMINATED);
    //  }

    if (workerExec != null) {
      workerExec.shutdown();
      workerExec.awaitTermination(5000, TimeUnit.MILLISECONDS);
    }

    writeStateUpdatesInternal(stateUpdates.keySet(), 1);
  }


  protected static byte[] toJavabin(Map<Integer,Integer> updates) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();

    try (JavaBinCodec codec = new JavaBinCodec()) {
      codec.marshal(updates, out);
    }
    return out.toByteArray();
  }

  private static class DocAssign {

    DocAssign() {
    }

    private final AtomicInteger replicaAssignCnt = new AtomicInteger();
  }

  public static class StateUpdate {
    public int id;
    public Slice.State state;
    public String sliceName;
    public String nodeName;
  }

  private void writeStateUpdatesInternal(Set<Integer> collIds, int tryCnt) {
    log.debug("writeStateUpdates for {}", collIds);
    for (Integer collId : collIds) {
      String collection = idToCollection.get(collId);

//      if (collection == null) {
//        log.info("could not find id for collection id={} collections={}", collId, getCollections());
//        if (TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS) < 5000) {
//
//          try {
//            writeStateUpdates(Collections.singleton(collId));
//          } catch (InterruptedException e) {
//
//          }
//
//        }
//        continue;
//      }

      overseer.getTaskZkWriterExecutor().submit(() -> {

        ActionThrottle writeThrottle = stateWriteThrottles.compute(collection, (s, throttle) -> {
          if (throttle == null) {
            return new ActionThrottle("zkstatewriter", Integer.getInteger("solr.zkstateWriteThrottle", 0));
          }
          return throttle;
        });

        writeThrottle.minimumWaitBetweenActions();
        writeThrottle.markAttemptingAction();

        String stateUpdatesPath = ZkStateReader.getCollectionStateUpdatesPath(collection);

        HashMap<Integer,Integer> javaBinMap = new HashMap<>(16);
        stateUpdates.compute(collId, (id, idToStateMap) -> {
          log.debug("writeStateUpdates for collection {} updates={}", collId, idToStateMap);
          if (idToStateMap == null) {
            idToStateMap = new ConcurrentHashMap<>(16);
          }

          javaBinMap.putAll(idToStateMap);

          return idToStateMap;
        });

        try {
          //  reader.getZkClient().setData(stateUpdatesPath, toJavabin(javaBinMap), -1, true);
          log.debug("writeStateUpdates for {} {}", collIds, javaBinMap);
          stateUpdateWrites.mark();
          reader.getZkClient().setData(stateUpdatesPath, toJavabin(javaBinMap), -1, (rc, path1, ctx, stat) -> {
            if (rc != 0) {
              KeeperException e = KeeperException.create(KeeperException.Code.get(rc), path1);
              log.error("Exception writeStateUpdates znode path={}", path1, e);

              if (e instanceof KeeperException.ConnectionLossException) {
                //                  reader.getZkClient().getConnectionManager().waitForConnected();
                //                  overseer.getTaskZkWriterExecutor().submit(() -> {
                //                    try {
                //                      workQueue.put(collIds);
                //                    } catch (InterruptedException interruptedException) {
                //                      log.warn("interrupted", interruptedException);
                //                    }
                //                  });

              }
            }
          }, "stateupdates");
        } catch (Exception e) {
          log.error("Exception writing out state updates async", e);

          if (e instanceof KeeperException.ConnectionLossException) {
            //              reader.getZkClient().getConnectionManager().waitForConnected();
            //              overseer.getTaskZkWriterExecutor().submit(() -> {
            //
            //                if (!workQueue.tryTransfer(collIds)) {
            //                  workQueue.put(collIds);
            //                }
            //              });

          }
        }
      });
    }

  }

  private final LinkedTransferQueue<Set<Integer>> workQueue = new LinkedTransferQueue<>();

  private volatile boolean terminated;
  private class Worker implements Runnable {

    Worker() {

    }

    @Override public void run() {

      while (!terminated) {
        try {
          Set<Integer> message = null;
          try {
            log.debug("ZkStateWriter worker will poll for 5 seconds");
            message = workQueue.poll(5000, TimeUnit.MILLISECONDS);
          } catch (InterruptedException e) {
            message = TERMINATED;
            terminated = true;
          } catch (Exception e) {
            log.warn("state publisher hit exception polling", e);
          }
          Set<Integer> bulkMessage = ConcurrentHashMap.newKeySet();
          if (message != null) {
            log.debug("Got state message {}", message);

            int pollTime;
            if (message == TERMINATED) {
              log.debug("State publish is terminated");
              terminated = true;
              pollTime = 0;
            } else {
              pollTime = bulkMessage(message, bulkMessage);
            }

            while (true) {
              try {
                message = workQueue.poll(pollTime, TimeUnit.MILLISECONDS);
              } catch (InterruptedException e) {
                message = TERMINATED;
                terminated = true;
              } catch (Exception e) {
                log.warn("zkstate writer hit exception polling", e);
              }
              if (message != null) {
                if (message == TERMINATED) {
                  terminated = true;
                  pollTime = 0;
                } else {
                  pollTime = bulkMessage(message, bulkMessage);
                }
              } else {
                break;
              }
            }
          }

          if (bulkMessage.size() > 0) {
            writeStateUpdatesInternal(bulkMessage, 1);
          }

        } catch (Exception e) {
          log.error("Exception in ZkStateWriter Worker run loop", e);
        }
      }
    }

    private int bulkMessage(Set<Integer> collIds, Set<Integer> bulkColIds) {
      bulkColIds.addAll(collIds);
      return 50;
    }
  }

}

