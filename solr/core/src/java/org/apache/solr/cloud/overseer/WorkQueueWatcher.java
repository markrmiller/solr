package org.apache.solr.cloud.overseer;

import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.StatePublisher;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.core.CoreContainer;
import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class WorkQueueWatcher extends QueueWatcher {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final Overseer overseer;

  private volatile boolean checkAgain = false;
  private volatile boolean running;

  public WorkQueueWatcher(Overseer overseer, CoreContainer cc) throws KeeperException {
    super(cc, overseer, Overseer.OVERSEER_QUEUE);
    this.overseer = overseer;
  }

  public void start(boolean weAreReplacement) throws KeeperException, InterruptedException {
    if (closed) return;

    zkController.getZkClient().addWatch(path, this, AddWatchMode.PERSISTENT);
    ourLock.lock();
    try {
      startItems = getItems(true);

      log.info("Overseer found entries on start {} {}", startItems, path);

      if (startItems.size() > 0) {
        processQueueItems(startItems, true, weAreReplacement);
      }
    } finally {
      ourLock.unlock();
    }
  }

  protected List<String> getItems(boolean onStart) {
    try {

      List<String> children = zkController.getZkClient().getChildren(path, null, null, true, false);

      log.debug("get items from Overseer state work queue onStart={} {} {}", onStart, path, children.size());

      List<String> items = new ArrayList<>(children);
      Collections.sort(items);
      return items;
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

    if (running) {
      checkAgain = true;
    } else {
      running = true;
      overseer.getTaskZkWriterExecutor().submit(() -> {
        Set<Long> collIds = new HashSet<>();
        try {
          do {

            List<String> items = getItems(false);
            try {

              if (items.size() > 0) {
                collIds.addAll(processQueueItems(items, false, false));
              }
            } catch (AlreadyClosedException e) {

            } catch (Exception e) {
              log.error("Exception during overseer queue queue processing", e);
            }

            if (!checkAgain) {
              running = false;
              break;
            }
            checkAgain = false;

          } while (true);

          doneWithCurrentQueue(collIds);
        } catch (Exception e) {
          log.error("exception submitting queue task", e);
        }

      });
    }

  }

  @Override
  protected Set<Long> processQueueItems(List<String> items, boolean onStart, boolean weAreReplacement) {
    //if (closed) return;
    if (items.size() == 0) {
      return Collections.emptySet();
    }
    List<String> fullPaths = new ArrayList<>(items.size());
    try {

      log.debug("Found state update queue items {}", items);

      for (String item : items) {
        fullPaths.add(path + "/" + item);
      }

      Map<String,byte[]> data = zkController.getZkClient().getData(fullPaths);

      Map<Long,Map<Long,String>> replicaStates = new LinkedHashMap<>();
      Map<Long,List<ZkStateWriter.StateUpdate>> sliceStates = new HashMap<>();

      Set<Long> collIds = new HashSet<>();
      data.forEach((key, value) -> {
        final ZkNodeProps message = ZkNodeProps.load(value);

        log.debug("add state update {}", message);

        final String op = message.getStr(StatePublisher.OPERATION);
        message.remove(StatePublisher.OPERATION);
        OverseerAction overseerAction = OverseerAction.get(op);
        switch (overseerAction) {
          case STATE:
            for (Object theEntry : message.entrySet()) {
              try {
                Map.Entry<String,Object> entry = (Map.Entry<String,Object>) theEntry;
                OverseerAction oa = OverseerAction.get(entry.getKey());

                if (OverseerAction.RECOVERYNODE.equals(oa) || OverseerAction.DOWNNODE.equals(oa)) {
                  if (OverseerAction.DOWNNODE.equals(oa) && onStart && !weAreReplacement) {
                    log.debug("Got DOWNNODE, but we are not a replacement leader, looks like a fresh start, ignoring ...");
                    continue;
                  }
                  ZkStateWriter.StateUpdate update = new ZkStateWriter.StateUpdate();
                  if (OverseerAction.DOWNNODE.equals(oa)) {
                    update.state = Replica.State.getShortState(Replica.State.DOWN);
                    log.debug("Process DOWNNODE for {} ... ", entry.getValue());
                  } else if (OverseerAction.RECOVERYNODE.equals(oa)) {
                    log.debug("Process RECOVERING for {} ... ", entry.getValue());
                    update.state = Replica.State.getShortState(Replica.State.RECOVERING);
                  }
                  update.nodeName = (String) entry.getValue();

                  Set<String> collections = overseer.getZkStateWriter().getCollections();

                  for (String collection : collections) {
                    ClusterState cs = overseer.getZkStateWriter().getClusterstate(collection);
                    if (cs != null) {
                      DocCollection docCollection = cs.getCollection(collection);
                      if (docCollection != null) {
                        List<Replica> replicas = docCollection.getReplicas();
                        for (Replica replica : replicas) {
                          if (replica.getNodeName().equals(update.nodeName)) {
                            Map<Long,String> updates = replicaStates.get(replica.getCollectionId());
                            collIds.add(docCollection.getId());
                            if (updates == null) {
                              updates = new LinkedHashMap<>();
                              replicaStates.put(replica.getCollectionId(), updates);
                            }
                            updates.put(replica.getInternalId(), update.state);
                          }
                        }
                      }
                    }
                  }
                  continue;
                }

                String id = entry.getKey();
                String stateString = (String) entry.getValue();
                long collId = Long.parseLong(id.substring(0, id.indexOf('-')));

                Map<Long,String> updates = replicaStates.get(collId);
                if (updates == null) {
                  updates = new LinkedHashMap<>();
                  collIds.add(collId);
                  replicaStates.put(collId, updates);
                }
                long replicaId = Long.parseLong(id.substring(id.indexOf('-') + 1));
                log.debug("add state update {} {} for collection {}", replicaId, stateString, collId);
                updates.put(replicaId, stateString);

              } catch (Exception e) {
                log.error("Overseer state update queue processing failed", e);
                fullPaths.remove(key);
              }
            }
            break;
          case UPDATESHARDSTATE:
            ZkStateWriter.StateUpdate update = new ZkStateWriter.StateUpdate();

            update.sliceState = message.getStr("state");
            update.sliceName = message.getStr("slice");
            update.id = message.getStr("id");

            List<ZkStateWriter.StateUpdate> updates = sliceStates.get(overseer.getId());
            if (updates == null) {
              updates = new ArrayList<>();
              sliceStates.put(Long.parseLong(overseer.getId()), updates);
            }
            updates.add(update);

            break;

          default:
            throw new RuntimeException("unknown operation:" + op + " contents:" + message);
        }
      });

      overseer.getZkStateWriter().enqueueStateUpdates(replicaStates, sliceStates);
      return collIds;

    } finally {

      if (fullPaths.size() > 0) {
        try {
          zkController.getZkClient().delete(fullPaths, true, true);
        } catch (Exception e) {
          log.warn("Failed deleting processed items", e);
        }
      }

    }
  }

  protected void doneWithCurrentQueue( Set<Long> collIds) {
    overseer.getZkStateWriter().writeStateUpdates(collIds);
  }
}
