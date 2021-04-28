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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.common.util.Utils;
import org.apache.solr.common.util.metrics.Metrics;
import org.apache.solr.core.CoreContainer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.jctools.maps.NonBlockingHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;

public class StatePublisher implements Closeable {
  private static final Logger log = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  static Meter cacheHits = Metrics.MARKS_METRICS.meter("statepublisher_cache_hits");
  static Meter published = Metrics.MARKS_METRICS.meter("statepublisher_published");


  public static final String OPERATION = "op";
  private volatile boolean closed;
  private volatile ExecutorService workerExec;

  private static class CacheEntry {
    Replica.State state;
    long time;
  }

  private final Map<String,CacheEntry> stateCache = new NonBlockingHashMap<>(16);
  private final ZkStateReader zkStateReader;
  private final CoreContainer cc;

  public static class NoOpMessage extends ZkNodeProps {
  }
  static final String PREFIX = "qn-";
  public static final NoOpMessage TERMINATE_OP = new NoOpMessage();
  public static final Map TERMINATE_OP_MAP = new HashMap(0);

  private final LinkedTransferQueue<Map> workQueue = new LinkedTransferQueue<>();

  private volatile Worker worker;

  private volatile boolean terminated;
  private class Worker implements Runnable {

    public static final int POLL_TIME_ON_PUBLISH_NODE = 1;
    public static final int POLL_TIME = 250;

    Worker() {

    }

    @Override
    public void run() {

      while (!terminated) {
        Map message = null;
        Map bulkMessage = new HashMap();
        bulkMessage.put(OPERATION, "state");
        int pollTime = 250;
        try {
          try {
            log.debug("State publisher will poll for 5 seconds");
            message = workQueue.poll(5000, TimeUnit.MILLISECONDS);
          } catch (InterruptedException e) {
            message = TERMINATE_OP_MAP;
            terminated = true;
          } catch (Exception e) {
            log.warn("state publisher hit exception polling", e);
          }
          if (message != null) {
            log.debug("Got state message " + message);

            if (message == TERMINATE_OP_MAP) {
              log.debug("State publish is terminated");
              message = TERMINATE_OP_MAP;
              terminated = true;
              pollTime = 1;
            } else {
              pollTime = bulkMessage(message, bulkMessage);
            }

            while (true) {
              try {
                log.debug("State publisher will poll for {} ms", pollTime);
                message = workQueue.poll(pollTime, TimeUnit.MILLISECONDS);
              } catch (InterruptedException e) {
                message = TERMINATE_OP_MAP;
                terminated = true;
              } catch (Exception e) {
                log.warn("state publisher hit exception polling", e);
              }
              if (message != null) {
                if (log.isDebugEnabled()) log.debug("Got state message " + message);
                if (message == TERMINATE_OP_MAP) {
                  terminated = true;
                  pollTime = 1;
                } else {
                  pollTime = bulkMessage(message, bulkMessage);
                }
              } else {
                break;
              }
            }
          }

          if (bulkMessage.size() > 1) {
            processMessage(bulkMessage);
          } else {
            log.debug("No messages to publish, loop");
          }

          if (terminated) {
            log.info("State publisher has terminated");
            break;
          }
        } catch (KeeperException.ConnectionLossException e) {
          log.warn("connection loss to zk", e);
          zkStateReader.getZkClient().getConnectionManager().waitForConnected();
        } catch (Exception e) {
          log.error("Exception in StatePublisher run loop", e);
        }
      }
    }

    private int bulkMessage(Map zkNodeProps, Map bulkMessage) {
      if (zkNodeProps.equals(TERMINATE_OP_MAP)) {
        return 0;
      }

      if (OverseerAction.get((String) zkNodeProps.get(OPERATION)) == OverseerAction.DOWNNODE) {
        String nodeName = (String) zkNodeProps.get(ZkStateReader.NODE_NAME_PROP);
        //clearStatesForNode(bulkMessage, nodeName);
        bulkMessage.put(OverseerAction.DOWNNODE.toLower(), nodeName);
        log.debug("add state to batch  down node, props={} result={}", zkNodeProps, bulkMessage);
        return 1;
      } else if (OverseerAction.get((String) zkNodeProps.get(OPERATION)) == OverseerAction.RECOVERYNODE) {
        log.debug("add state to batch  recovery node, props={} result={}", zkNodeProps, bulkMessage);
        String nodeName = (String) zkNodeProps.get(ZkStateReader.NODE_NAME_PROP);
       // clearStatesForNode(bulkMessage, nodeName);
        bulkMessage.put(OverseerAction.RECOVERYNODE.toLower(), nodeName);
        log.debug("add state to batch  recovery node, props={} result={}" , zkNodeProps, bulkMessage);
        return 1;
      } else {
        //String collection = zkNodeProps.getStr(ZkStateReader.COLLECTION_PROP);
        String core = (String) zkNodeProps.get(ZkStateReader.CORE_NAME_PROP);
        String id = (String) zkNodeProps.get("id");
        Replica.State state = (Replica.State) zkNodeProps.get(ZkStateReader.STATE_PROP);

        if (state == null) {
          log.error("Found null state in state update message={}", zkNodeProps);
          return 50;
        }

        Integer line = Replica.State.getShortState(state);
        if (log.isDebugEnabled()) log.debug("add state to batch core={} id={} state={} line={}", core, id, state, line);
        bulkMessage.put(id, line);
        if (state == Replica.State.LEADER) {
          return 1;
        } else if (state == Replica.State.ACTIVE) {
          return 25;
        } else {
          return 50;
        }
      }

    }

//    private void clearStatesForNode(ZkNodeProps bulkMessage, String nodeName) {
//      Set<String> removeIds = new HashSet<>();
//      Set<String> ids = bulkMessage.getProperties().keySet();
//      for (String id : ids) {
//        if (id.equals(OverseerAction.DOWNNODE.toLower()) || id.equals(OverseerAction.RECOVERYNODE.toLower())) {
//          continue;
//        }
//        Collection<DocCollection> collections = zkStateReader.getClusterState().getCollectionsMap().values();
//        for (DocCollection collection : collections) {
//          Replica replica = collection.getReplicaById(id);
//          if (replica != null) {
//            if (replica.getNodeName().equals(nodeName)) {
//              removeIds.add(id);)o(
//            }
//          }
//        }
//
//      }
//      for (String id : removeIds) {
//        bulkMessage.getProperties().remove(id);
//      }
//    }

    private void processMessage(Map message) throws KeeperException, InterruptedException {
      log.debug("Send state updates to Overseer {}", message);
      byte[] updates = Utils.toJSON(message);
      try {
        zkStateReader.getZkClient().create("/overseer/queue" + '/' + PREFIX, updates, CreateMode.PERSISTENT_SEQUENTIAL, (rc, path, ctx, name, stat) -> {
          if (rc != 0) {

            KeeperException e = KeeperException.create(KeeperException.Code.get(rc), path);
            log.error("Exception publish state messages path=" + path, e);

            if (e instanceof KeeperException.ConnectionLossException) {
              // if we use, check for alive
//              zkStateReader.getZkClient().getConnectionManager().waitForConnected();
//              workQueue.offer(message);
            }
          }
        });
      } catch (KeeperException.ConnectionLossException e) {
        log.error("Exception publish state messages", e);
        // if we use, check for alive
        //              zkStateReader.getZkClient().getConnectionManager().waitForConnected();
        //              workQueue.offer(message);
//        zkStateReader.getZkClient().getConnectionManager().waitForConnected();
//        workQueue.offer(message);
      }
    }
  }

  public StatePublisher(ZkStateReader zkStateReader, CoreContainer cc) {
    this.zkStateReader = zkStateReader;
    this.cc = cc;
  }

  public void submitState(ZkNodeProps stateMessage) {
    // Don't allow publish of state we last published if not DOWNNODE?
    try {
      if (stateMessage != TERMINATE_OP) {
        published.mark();
        String operation = stateMessage.getStr(OPERATION);
        String id = null;
        if (operation.equals("state")) {
          String core = stateMessage.getStr(ZkStateReader.CORE_NAME_PROP);
          String collection = stateMessage.getStr(ZkStateReader.COLLECTION_PROP);
          Replica.State state = (Replica.State) stateMessage.get(ZkStateReader.STATE_PROP);

          log.debug("submit state for publishing core={} state={}", core, state);

          if (core == null || state == null) {
            log.error("Nulls in published state");
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Nulls in published state " + stateMessage);
          }

          DocCollection coll = zkStateReader.getCollectionOrNull(collection);

          if (coll != null) {
            Replica replica = coll.getReplica(core);
            if (replica != null) {
              id = replica.getId();
            } else {
              id = stateMessage.getStr("id");
            }

          }

          if (id == null) {
            id = stateMessage.getStr("id");
          } else {
            stateMessage.getProperties().putIfAbsent("id", id);
          }

          CacheEntry lastState = stateCache.get(id);
          if (lastState != null && state.equals(lastState.state)) {
            cacheHits.mark();
            log.info("Skipping publish state as {} for {}, because it was the last state published", state, core);
            return;
          }

          if (state == null) {
            log.error("Null state passed to publish {}", stateMessage);
            throw new IllegalArgumentException("Null state passed to publish " + stateMessage);
          }

          CacheEntry cacheEntry = new CacheEntry();
          cacheEntry.time = System.currentTimeMillis();
          cacheEntry.state = state;
          stateCache.put(id, cacheEntry);

          //        else if (operation.equalsIgnoreCase(OverseerAction.DOWNNODE.toLower())) {
          //          // set all statecache entries for replica to a state
          //
          //          Collection<CoreDescriptor> cds = cc.getCoreDescriptors();
          //          for (CoreDescriptor cd : cds) {
          //            DocCollection doc = zkStateReader.getCollectionOrNull(cd.getCollectionName());
          //            Replica replica = null;
          //            if (doc != null) {
          //              replica = doc.getReplica(cd.getName());
          //
          //              if (replica != null) {
          //                CacheEntry cacheEntry = new CacheEntry();
          //                cacheEntry.time = System.currentTimeMillis();
          //                cacheEntry.state = Replica.State.getShortState(Replica.State.DOWN);
          //                stateCache.put(replica.getId(), cacheEntry);
          //              }
          //            }
          //          }
          //
          //        } else if (operation.equalsIgnoreCase(OverseerAction.RECOVERYNODE.toLower())) {
          //          // set all statecache entries for replica to a state
          //
          //          Collection<CoreDescriptor> cds = cc.getCoreDescriptors();
          //          for (CoreDescriptor cd : cds) {
          //            DocCollection doc = zkStateReader.getCollectionOrNull(cd.getCollectionName());
          //            Replica replica = null;
          //            if (doc != null) {
          //              replica = doc.getReplica(cd.getName());
          //
          //              if (replica != null) {
          //                CacheEntry cacheEntry = new CacheEntry();
          //                cacheEntry.time = System.currentTimeMillis();
          //                cacheEntry.state = Replica.State.getShortState(Replica.State.RECOVERING);
          //                stateCache.put(replica.getId(), cacheEntry);
          //              }
          //            }
        }
      }
      //      else {
      //        log.error("illegal state message {}", stateMessage.toString());
      //        throw new IllegalArgumentException(stateMessage.toString());
      //      }
      //

      if (stateMessage == TERMINATE_OP) {
        //if (!workQueue.tryTransfer(TERMINATE_OP_MAP)) {
          workQueue.put(TERMINATE_OP_MAP);
     //   }
      } else {
        Map<String,Object> props = stateMessage.getProperties();
      //  if (!workQueue.tryTransfer(props)) {
          workQueue.put(props);
      //  }
      }
    } catch (Exception e) {
      log.error("Exception trying to publish state message={}", stateMessage, e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }

  }

    public void clearStatCache (String core){
      stateCache.remove(core);
    }

    public void clearStatCache () {
      stateCache.clear();
    }

    public void start () {
      this.worker = new Worker();
      workerExec = Executors.newSingleThreadExecutor(new SolrNamedThreadFactory("StatePublisher", true));

      workerExec.submit(this.worker);
    }

    public void close () {
     // this.closed = true;

     // if (!workQueue.tryTransfer(TERMINATE_OP_MAP)) {
        workQueue.put(TERMINATE_OP_MAP);
      //}

      workerExec.shutdown();
      try {
        workerExec.awaitTermination(5000, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {

      }
    }
  }
