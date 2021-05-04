package org.apache.solr.cloud.overseer;

import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.StatePublisher;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.core.CoreContainer;
import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.jctools.maps.NonBlockingHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.locks.ReentrantLock;

public class WorkQueueWatcher extends QueueWatcher {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final Overseer overseer;

  private volatile boolean checkAgain = false;
  private volatile boolean running;

  private final ReentrantLock lock = new ReentrantLock();

  public WorkQueueWatcher(Overseer overseer, CoreContainer cc) throws KeeperException {
    super(cc, overseer, Overseer.OVERSEER_QUEUE);
    this.overseer = overseer;
  }

  public void start(boolean weAreReplacement) throws KeeperException, InterruptedException {
    if (closed) return;
    zkController.getZkClient().addWatch(path, this, AddWatchMode.PERSISTENT);
    Queue<String> startItems = getItems(true);
    log.info("Overseer found entries on start {} {}", startItems, path);
    if (startItems.size() > 0) {
      processQueueItems(startItems, true, weAreReplacement);
    }

  }

  protected Queue<String> getItems(boolean onStart) {
    try {
      List<String> children = zkController.getZkClient().getChildren(path, null, null, true, false);
      if (log.isDebugEnabled()) {
        log.debug("get items from Overseer state work queue onStart={} {} {}", onStart, path, children.size());
      }
      List<String> items = new ArrayList<>(children);
      Collections.sort(items);
      log.debug("sorted state items from zk queue={}", items);
      return new LinkedTransferQueue<>(items);
    } catch (KeeperException.SessionExpiredException e) {
      log.warn("ZooKeeper session expired");
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    } catch (AlreadyClosedException e) {
      throw e;
    } catch (Exception e) {
      log.error("Unexpected error in Overseer state update loop", e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  @Override
  public void processEvent(WatchedEvent event) {
    if (!Event.EventType.NodeChildrenChanged.equals(event.getType())) {
      return;
    }
    if (this.closed || zkController.getZkClient().isClosed()) {
      log.info("Overseer is closed, do not process watcher for queue");
      return;
    }

    lock.lock();
    try {

      if (running) {
        checkAgain = true;
      } else {
        running = true;
        overseer.getTaskZkWriterExecutor().submit(() -> {
          try {
            do {
              checkAgain = false;
              Queue items = getItems(false);
              try {

                if (items.size() > 0) {
                  processQueueItems(items, false, false);
                }
              } catch (AlreadyClosedException e) {

              } catch (Exception e) {
                log.error("Exception during overseer queue queue processing", e);
              }

              if (!checkAgain) {
                running = false;
                break;
              }

            } while (true);

          } catch (Exception e) {
            log.error("exception submitting queue task", e);
          }
        });
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  protected Set<Integer> processQueueItems(Queue<String> items, boolean onStart, boolean weAreReplacement) {
    if (items.size() == 0) {
      return Collections.emptySet();
    }
    List<String> fullPaths = new ArrayList<>(items.size());

    log.debug("Found state update queue items {}", items);

    for (String item : items) {
      fullPaths.add(path + '/' + item);
    }

    Map<String,byte[]> data = zkController.getZkClient().getData(fullPaths);
    Set<Integer> collIds = new HashSet<>();
    data.forEach((key, value) -> {
      Map<Integer,Map<Integer,Integer>> replicaStates = new ConcurrentHashMap<>();
      Map<Integer,List<ZkStateWriter.StateUpdate>> sliceStates = new ConcurrentHashMap<>();
      final ZkNodeProps message = ZkNodeProps.load(value);

      log.debug("add state update {}", message);

      final String op = message.getStr(StatePublisher.OPERATION);
      message.getProperties().remove(StatePublisher.OPERATION);
      OverseerAction overseerAction = OverseerAction.get(op);
      switch (overseerAction) {
        case STATE:
          processStateUpdateNode(onStart, weAreReplacement, collIds, replicaStates, message);
          processStateUpdateReplica(onStart, weAreReplacement, collIds, replicaStates, message);
          break;
        case UPDATESHARDSTATE:
          try {
            int id = ((Long) message.getProperties().remove("id")).intValue();

            List<ZkStateWriter.StateUpdate> updates = sliceStates.computeIfAbsent(id, k -> new ArrayList<>());

            Set<Map.Entry<String,Object>> entries = message.getProperties().entrySet();
            for (Map.Entry<String,Object> entry : entries) {
              ZkStateWriter.StateUpdate update = new ZkStateWriter.StateUpdate();
              update.sliceName = entry.getKey();
              update.state = Slice.State.getState((String) entry.getValue());
              updates.add(update);
            }
          } catch (Exception e) {
            log.error("Overseer slice state update queue processing failed {}", message, e);
          }
          break;

        default:
          throw new RuntimeException("unknown operation:" + op + " contents:" + message);
      }
      overseer.getZkStateWriter().enqueueStateUpdates(replicaStates, sliceStates);
      try {
        overseer.getZkStateWriter().writeStateUpdates(collIds);
      } catch (InterruptedException e) {
        log.warn("interrupted", e);
        throw new AlreadyClosedException(e);
      }

      if (!overseer.getTaskZkWriterExecutor().isShutdown()) {
        try {
          zkController.getZkClient().delete(key, -1);
        } catch (Exception e) {
          log.warn("Failed deleting processed items", e);
        }
      }
    });

    return collIds;
  }

  private void processStateUpdateNode(boolean onStart, boolean weAreReplacement, Set<Integer> collIds, Map<Integer,Map<Integer,Integer>> replicaStates,
      ZkNodeProps message) {
    for (Map.Entry<String,Object> theEntry : message.getProperties().entrySet()) {
      Map.Entry<String,Object> entry = theEntry;
      log.debug("process state update entry {}", entry);
      try {

        if (OverseerAction.RECOVERYNODE.toLower().equals(entry.getKey()) || OverseerAction.DOWNNODE.toLower().equals(entry.getKey())) {
          if (onStart && !weAreReplacement) {
            if (log.isDebugEnabled()) {
              log.debug("Got {}, but we are not a replacement leader, looks like a fresh start, ignoring ...", entry.getKey());
            }
            continue;
          }

          Integer state = null;
          if (OverseerAction.DOWNNODE.toLower().equals(entry.getKey())) {
            state = Replica.State.getShortState(Replica.State.DOWN);
            if (log.isDebugEnabled()) {
              log.debug("Process DOWNNODE for {} ... ", entry.getValue());
            }
          } else if (OverseerAction.RECOVERYNODE.toLower().equals(entry.getKey())) {
            if (log.isDebugEnabled()) {
              log.debug("Process RECOVERING for {} ... ", entry.getValue());
            }
            state = Replica.State.getShortState(Replica.State.RECOVERING);
          }
          String nodeName = (String) entry.getValue();

          Set<String> collections = overseer.getZkStateWriter().getCollections();

          for (String collection : collections) {
            ClusterState cs = overseer.getZkStateWriter().getClusterstate(collection);
            if (cs != null) {
              DocCollection docCollection = cs.getCollection(collection);
              if (docCollection != null) {
                List<Replica> replicas = docCollection.getReplicas();
                for (Replica replica : replicas) {
                  if (replica.getNodeName().equals(nodeName)) {
                    collIds.add(docCollection.getId());
                    Map<Integer,Integer> updates = replicaStates.computeIfAbsent(replica.getCollectionId(), k -> new ConcurrentHashMap<>());
                    if (log.isDebugEnabled()) {
                      log.debug("add state update id={} {} for collection {}", replica.getInternalId(), state, docCollection.getId());
                    }
                    updates.remove(replica.getInternalId());
                    updates.put(replica.getInternalId(), state);
                  }
                }
              }
            }
          }
          continue;
        }
      } catch (Exception e) {
        log.error("Overseer state update queue processing failed entry-{}", theEntry, e);
      }

    }
  }

  private void processStateUpdateReplica(boolean onStart, boolean weAreReplacement, Set<Integer> collIds, Map<Integer,Map<Integer,Integer>> replicaStates,
      ZkNodeProps message) {
    for (Map.Entry<String,Object> theEntry : message.getProperties().entrySet()) {
      Map.Entry<String,Object> entry = theEntry;
      log.debug("process state update entry {}", entry);
      try {

        if (OverseerAction.RECOVERYNODE.toLower().equals(entry.getKey()) || OverseerAction.DOWNNODE.toLower().equals(entry.getKey())) {
          continue;
        }
      } catch (Exception e) {
        log.error("Overseer state update queue processing failed entry-{}", theEntry, e);
      }

      try {
        String id = entry.getKey();
        Integer state = ((Long) entry.getValue()).intValue();
        int collId = Integer.parseInt(id.substring(0, id.indexOf('-')));

        Map<Integer,Integer> updates = replicaStates.computeIfAbsent(collId, k -> new NonBlockingHashMap<>());
        collIds.add(collId);
        Integer replicaId = Integer.parseInt(id.substring(id.indexOf('-') + 1));
        log.debug("add state update id={} {} for collection {}", replicaId, state, collId);
        updates.remove(replicaId);
        updates.put(replicaId, state);

      } catch (Exception e) {
        log.error("Overseer state update queue processing failed entry={}", theEntry, e);
      }
    }
  }
}
