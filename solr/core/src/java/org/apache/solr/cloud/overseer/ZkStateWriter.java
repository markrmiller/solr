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
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;

import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.Stats;
import org.apache.solr.cloud.api.collections.Assign;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.FastOutputStream;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.Utils;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.singletonMap;

public class ZkStateWriter {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static final String CS_VER_ = "_cs_ver_";
  private final ZkStateReader reader;
  private final Overseer overseer;

  /**
   * Represents a no-op {@link ZkWriteCommand} which will result in no modification to cluster state
   */

  protected volatile Stats stats;

  private final ConcurrentHashMap<Long, Map<Long,String>> stateUpdates = new ConcurrentHashMap<>();

  Map<Long,List<ZkStateWriter.StateUpdate>> sliceStates = new ConcurrentHashMap<>();

  private final Map<Long,String> idToCollection = new ConcurrentHashMap<>(128, 0.75f, 16);

  private final Map<String,DocAssign> assignMap = new ConcurrentHashMap<>(128, 0.75f, 16);

  private final Map<String,ReentrantLock> collLocks = new ConcurrentHashMap<>(128, 0.75f, 16);

  private final Map<String,DocCollection> cs = new ConcurrentHashMap<>(128, 0.75f, 16);


  private AtomicLong ID = new AtomicLong();

  private Set<String> dirtyStructure = ConcurrentHashMap.newKeySet();

  public ZkStateWriter(ZkStateReader zkStateReader, Stats stats, Overseer overseer) {
    this.overseer = overseer;
    this.reader = zkStateReader;
    this.stats = stats;

  }

  public void enqueueStateUpdates(Map<Long,Map<Long,String>> replicaStates,  Map<Long,List<ZkStateWriter.StateUpdate>> sliceStates) {
    log.debug("enqueue state updates");

    replicaStates.forEach((aLong, longStringMap) -> {
     // log.debug("enqueue state updates for {} {}", replicaStatesEntry.getKey(), replicaStatesEntry.getValue());
      // this.stateUpdates.compute(replicaStatesEntry.getKey(), aLong -> replicaStatesEntry.getValue());
      this.stateUpdates.compute(aLong, (id, map) -> {
        if (map == null) {
          return new ConcurrentHashMap<>(longStringMap);
        }
        map.putAll(longStringMap);

        return map;
      });

     // log.debug("enqueue state updates result {} {}", replicaStatesEntry.getKey(), stateUpdates.get(replicaStatesEntry.getKey()));
    });
  }

  public void enqueueStructureChange(DocCollection docCollection) throws Exception {
    if (overseer.isClosed()) {
      log.info("Overseer is closed, do not process watcher for queue");
      throw new AlreadyClosedException();
    }
    try {

      log.debug("enqueue structure change docCollection={} replicas={}", docCollection, docCollection.getReplicas());

      String collectionName = docCollection.getName();
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
        idToCollection.put(docCollection.getId(), docCollection.getName());

        Map<Long,String> updates = new HashMap<>();
        this.stateUpdates.compute(docCollection.getId(), (k, v) -> {
          if (v == null) {
            return new ConcurrentHashMap<>();
          }
          updates.putAll(v);
          return v;
        });

        if (currentCollection != null) {
          List<String> removeSlices = new ArrayList();
          List<String> removeReplicas = new ArrayList();
          List<Slice> updatedSlices = new ArrayList();
//          for (Slice slice : currentCollection) {
//            Slice updatedSlice = docCollection.getSlice(slice.getName());
//
//            if (updatedSlice.getBool("remove", false)) {
//              removeSlices.add(slice.getName());
//            }
//
//            if (updatedSlice.getState() != slice.getState()) {
//              Map newProps = new HashMap(2);
//              newProps.put(ZkStateReader.STATE_PROP, updatedSlice.getState().toString());
//              updatedSlices.add(slice.copy(newProps));
//            }
//          }

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
              String s = updates.get(replica.getInternalId());
              if (s != null) {
                Map newProps = new HashMap(2);
                if (s.equals("l")) {
                  newProps.put("leader", "true");
                  newProps.put(ZkStateReader.STATE_PROP, Replica.State.ACTIVE.toString());
                } else {
                  newProps.put(ZkStateReader.STATE_PROP, Replica.State.shortStateToState(s).toString());
                  newProps.remove("leader");
                }
                Replica newReplica = replica.copyWithProps(newProps);
                newReplicaMap.put(replica.getName(), newReplica);
              }
            }

            for (String removeReplica : removeReplicas) {
              newReplicaMap.remove(removeReplica);
            }

            Slice newDocCollectionSlice = docCollectionSlice.copyWithReplicas(newReplicaMap);

            newSlices.put(newDocCollectionSlice.getName(), newDocCollectionSlice);
          }

          for (String removeSlice : removeSlices) {
            newSlices.remove(removeSlice);
          }

          Map newDocProps = new HashMap(currentCollection);
          newDocProps.forEach((k, v) -> newDocProps.putIfAbsent(k, v));
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
          Map newDocProps = new HashMap(docCollection);

          Map<String,Slice> newSlices = docCollection.getSlicesCopy();
          List<String> removeSlices = new ArrayList();
          for (Slice slice : docCollection) {

            if (slice.get("remove") != null) {
              removeSlices.add(slice.getName());
            }

            for (Replica replica : slice.getReplicas()) {
              String s = updates.get(replica.getInternalId());
              if (s != null) {
                Map<String,Replica> newReplicaMap = slice.getReplicasCopy();
                Map<Object,Object> newProps = new HashMap(2);

                if (s.equals("l")) {
                  newProps.put("leader", "true");
                  newProps.put(ZkStateReader.STATE_PROP, Replica.State.ACTIVE.toString());
                } else {
                  newProps.put(ZkStateReader.STATE_PROP, Replica.State.shortStateToState(s).toString());
                  newProps.remove("leader");
                }
                Replica newReplica = replica.copyWithProps(newProps);
                newReplicaMap.put(newReplica.getName(), newReplica);
                Slice newSlice = slice.copyWithReplicas(newReplicaMap);
                newSlices.put(newSlice.getName(), newSlice);
              }
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

  public Future writePendingUpdates(String collection) {
    return ParWork.getRootSharedExecutor().submit(() -> {
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

      } while (!overseer.isClosed() && !overseer.getZkStateReader().getZkClient().isClosed());
    });
  }

  private void write(String coll) throws KeeperException.BadVersionException {

    if (log.isDebugEnabled()) {
      log.debug("writePendingUpdates {}", coll);
    }

    log.debug("process collection {}", coll);
    ReentrantLock collLock = collLocks.compute(coll, (s, reentrantLock) -> {
      if (reentrantLock == null) {
        return new ReentrantLock();
      }
      return reentrantLock;
    });
    collLock.lock();
    try {

      DocCollection collection = cs.get(coll);

      if (collection == null) {
        return;
      }

      collection = cs.get(coll);

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
          }
          if (reader.getZkClient() == null) {
            log.error("zkclient not initialized in zkstatewriter");
          }

            dirtyStructure.remove(collection.getName());

            DocCollection finalCollection = collection;
            reader.getZkClient().setData(path, data, -1, (rc, path1, ctx, stateJsonStat) -> {
              if (rc != 0) {
                KeeperException e = KeeperException.create(KeeperException.Code.get(rc), path1);
                log.error("Exception on trigger scn znode path=" + path1, e);

                if (e instanceof KeeperException.BadVersionException) {

                  log.info("Tried to update state.json for {} with bad version {} \n {}", coll, -1, finalCollection);
                  ReentrantLock collLock2 = collLocks.compute(coll, (s, reentrantLock) -> {
                    if (reentrantLock == null) {
                      return new ReentrantLock();
                    }
                    return reentrantLock;
                  });
                  collLock2.lock();
                  try {
                    DocCollection docColl = reader.getCollectionLive(coll);
                    idToCollection.put(docColl.getId(), docColl.getName());
                    cs.put(coll, docColl);
                    dirtyStructure.add(coll);
                  } finally {
                    collLock2.unlock();
                  }
                  overseer.getZkStateWriter().writePendingUpdates(coll);
                }
                if (e instanceof KeeperException.ConnectionLossException) {
                  dirtyStructure.add(coll);
                  overseer.getZkStateWriter().writePendingUpdates(coll);
                }

              } else {
                try {
                  reader.getZkClient().setData(pathSCN, null, -1, (rc2, path2, ctx2, scnStat) -> {
                    if (rc2 != 0) {
                      KeeperException e = KeeperException.create(KeeperException.Code.get(rc2), path2);
                      log.error("Exception on trigger scn znode path=" + path2, e);
                    }
                  }, "pathSCN");
                } catch (Exception e) {
                  log.error("Exception triggering SCN node");
                }
              }
            }, "state.json");

        }

      } catch (Exception e) {
        log.error("Failed processing update=" + collection, e);
      }

    } finally {
      collLock.unlock();
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

  public Long getIdForCollection(String collection) {
    ReentrantLock collectionLock = collLocks.get(collection);
    if (collectionLock == null) {
      return null;
    }
    collectionLock.lock();
    try {
      return cs.get(collection).getId();
    } finally {
      collectionLock.unlock();
    }
  }

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
        Long id = null;
        for (Map.Entry<Long,String> entry : idToCollection.entrySet()) {
          if (entry.getValue().equals(collection)) {
            id = entry.getKey();
            break;
          }
        }
        if (id != null) {
          idToCollection.remove(id);
        }
        stateUpdates.remove(collection);
        DocCollection doc = cs.get(collection);

        if (doc != null) {
          List<Replica> replicas = doc.getReplicas();
          for (Replica replica : replicas) {
            overseer.getCoreContainer().getZkController().clearCachedState(replica.getName());
          }
          idToCollection.remove(doc.getId());
        }

        cs.remove(collection);
        assignMap.remove(collection);
        dirtyStructure.remove(collection);

      } finally {
        collLock.unlock();
      }
    } catch (Exception e) {
      log.error("Exception removing collection", e);

    }
  }

  public long getHighestId(String collection) {
    long id = ID.incrementAndGet();
    idToCollection.put(id, collection);
    return id;
  }

  public int getReplicaAssignCnt(String collection, String shard, String namePrefix) {
    DocAssign docAssign = assignMap.get(collection);

    docAssign = assignMap.computeIfAbsent(collection, c ->  new DocAssign(collection));

    int id = docAssign.replicaAssignCnt.incrementAndGet();
    log.debug("assign id={} for collection={} slice={} namePrefix={}", id, collection, shard, namePrefix);
    return id;
  }

  public void init(boolean weAreReplacement) {
    log.info("ZkStateWriter Init - A new Overseer in charge or we are back baby replacement={}", weAreReplacement);
    try {
      overseer.getCoreContainer().getZkController().clearStatePublisher();
      ClusterState readerState = reader.getClusterState();
      Set<String> collectionNames = new HashSet<>(readerState.getCollectionsMap().size());
      if (readerState != null) {
        readerState.forEachCollection(docCollection -> collectionNames.add(docCollection.getName()));
      }

      long[] highId = new long[1];
      collectionNames.forEach(collectionName -> {

        ReentrantLock collLock = collLocks.compute(collectionName, (s, reentrantLock) -> {
          if (reentrantLock == null) {
            return new ReentrantLock();
          }
          return reentrantLock;
        });
        collLock.lock();
        try {

          DocCollection docCollection = reader.getCollection(collectionName);

          Map<Long,String> latestStateUpdates = docCollection.getStateUpdates();

          if (weAreReplacement) {
            final Map existingUpdates = new ConcurrentHashMap(latestStateUpdates);
            existingUpdates.remove("_ver_");
            stateUpdates.compute(docCollection.getId(), (k, v) -> {
           //   if (v == null) {
                return existingUpdates;
           //   }

           //   return v;
            });
          } else {
            log.debug("Clear current state updates as we are not replacing an overseer from the election line");
            stateUpdates.clear();
            writeStateUpdates(Collections.singleton(docCollection.getId()));
          }

          cs.put(collectionName, docCollection);

          if (docCollection.getId() > highId[0]) {
            highId[0] = docCollection.getId();
          }

          idToCollection.put(docCollection.getId(), docCollection.getName());

          DocAssign docAssign = new DocAssign(collectionName);
          assignMap.put(collectionName, docAssign);
          int max = 1;
          Collection<Slice> slices = docCollection.getSlices();
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
        } finally {
          collLock.unlock();
        }
      });

      ID.set(highId[0]);

      if (log.isDebugEnabled()) log.debug("zkStateWriter starting with cs {}", cs);
    } catch (Exception e) {
      log.error("Exception in ZkStateWriter init", e);
    }
  }

  public Set<String> getCollections() {
    return cs.keySet();
  }

  public void writeStateUpdates(Set<Long> collIds) {
    log.debug("writeStateUpdates for {}", collIds);
    for (Long collId : collIds) {
      String collection = idToCollection.get(collId);
      if (collection != null) {

        String stateUpdatesPath = ZkStateReader.getCollectionStateUpdatesPath(collection);
        LinkedHashMap<Long,String> updates = new LinkedHashMap(2);

        updates.putAll(stateUpdates.get(collId));
        log.debug("writeStateUpdates for collection {} updates={}", collId, updates);
        if (updates.size() == 0) {
          continue;
        }
        try {
          reader.getZkClient().setData(stateUpdatesPath, toJavabin(updates), -1, (rc, path1, ctx, stat) -> {
            if (rc != 0) {
              KeeperException e = KeeperException.create(KeeperException.Code.get(rc), path1);
              log.error("Exception writeStateUpdates znode path=" + path1, e);

              if (e instanceof KeeperException.ConnectionLossException) {
                ParWork.getRootSharedExecutor().submit(() -> {
                  overseer.getZkStateWriter().writeStateUpdates(Collections.singleton(collId));
                });

              }
            }
          }, "stateupdates");
        } catch (Exception e) {
          log.error("Exception writing out state updates async", e);
        }
      }
    }
  }

  private static final byte VERSION = 2;

  protected byte[] toJavabin(Map<Long,String> updates) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    FastOutputStream fastOutputStream = new FastOutputStream(baos);

    try (JavaBinCodec codec = new JavaBinCodec()) {
      codec.marshal(updates, fastOutputStream);
    }
    fastOutputStream.flushBuffer();
    return baos.toByteArray();
  }

  private static class DocAssign {
    private final String collection;

    DocAssign(String collection) {
      this.collection = collection;
    }

    private final AtomicInteger replicaAssignCnt = new AtomicInteger();
  }

  public static class StateUpdate {
    public volatile String id;
    public volatile String state;
    public volatile String sliceState;
    public volatile String sliceName;
    public volatile String nodeName;
  }

}

