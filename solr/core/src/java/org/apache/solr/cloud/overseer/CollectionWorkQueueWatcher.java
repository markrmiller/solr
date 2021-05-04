package org.apache.solr.cloud.overseer;

import org.apache.solr.client.solrj.impl.LBHttp2SolrClient;
import org.apache.solr.cloud.DistributedMap;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.OverseerConfigSetMessageHandler;
import org.apache.solr.cloud.OverseerSolrResponse;
import org.apache.solr.cloud.OverseerSolrResponseSerializer;
import org.apache.solr.cloud.OverseerTaskQueue;
import org.apache.solr.cloud.Stats;
import org.apache.solr.cloud.api.collections.OverseerCollectionMessageHandler;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.OverseerConfigSetMessageHandler.CONFIGSETS_ACTION_PREFIX;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedTransferQueue;

public class CollectionWorkQueueWatcher extends QueueWatcher {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final OverseerCollectionMessageHandler collMessageHandler;
  private final OverseerConfigSetMessageHandler configMessageHandler;
  private final DistributedMap failureMap;
  private final DistributedMap runningMap;

  private final DistributedMap completedMap;

 // private final ReentrantLock ourLock = new ReentrantLock(false);

  private volatile boolean checkAgain = false;
  private volatile boolean running;

  public CollectionWorkQueueWatcher(CoreContainer cc, Integer myId, LBHttp2SolrClient overseerLbClient, String adminPath, Stats stats, Overseer overseer)
      throws KeeperException {
    super(cc, overseer, Overseer.OVERSEER_COLLECTION_QUEUE_WORK);
    collMessageHandler = new OverseerCollectionMessageHandler(cc, myId, overseerLbClient, adminPath, stats, overseer);
    configMessageHandler = new OverseerConfigSetMessageHandler(cc);
    failureMap = Overseer.getFailureMap(cc.getZkController().getZkClient());
    runningMap = Overseer.getRunningMap(cc.getZkController().getZkClient());
    completedMap = Overseer.getCompletedMap(cc.getZkController().getZkClient());
  }

  @Override
  public void close() {
    super.close();
    IOUtils.closeQuietly(collMessageHandler);
    IOUtils.closeQuietly(configMessageHandler);
  }

  @Override
  public void start(boolean weAreReplacement) throws KeeperException, InterruptedException {
    if (closed) return;

    zkController.getZkClient().addWatch(path, this, AddWatchMode.PERSISTENT);

    Queue<String> startItems = getItems();

    log.info("Overseer found entries on start {}", startItems);
    if (startItems.size() > 0) {
      processQueueItems(startItems, true, weAreReplacement);
    }

  }

  protected Queue<String> getItems() throws KeeperException.SessionExpiredException {
    try {

      if (log.isDebugEnabled()) log.debug("get items from Overseer Collection work queue {}", path);

      List<String> children = zkController.getZkClient().getChildren(path, null, null, true, false);

      List<String> items = new ArrayList<>(children);
      Collections.sort(items);
      return new LinkedTransferQueue<>(items);
    } catch (AlreadyClosedException | KeeperException.SessionExpiredException e) {
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

   // ourLock.lock();
    try {
      if (running) {
        checkAgain = true;
      } else {
        running = true;
        overseer.getCollectionQueueTaskExecutor().submit(() -> {
          try {
            do {
              try {
                checkAgain = false;
                Queue items = getItems();

                if (items.size() > 0) {
                  processQueueItems(items, false, false);
                }
              } catch (AlreadyClosedException e) {
                return;
              } catch (KeeperException.SessionExpiredException e) {
                log.info("Session expire for Overseer collection queue");
                return;
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
     // ourLock.unlock();
    }
  }

  @Override
  protected Set<Integer> processQueueItems(Queue<String> items, boolean onStart, boolean weAreReplacement) {
    if (closed) return null;

    List<String> fullPaths = new ArrayList<>(items.size());

    log.info("Found collection queue items {} onStart={}", items, onStart);
    for (String item : items) {
      fullPaths.add(path + '/' + item);
    }
    List<Future> futures = new ArrayList<>(fullPaths.size());

    Map<String,byte[]> data = zkController.getZkClient().getData(fullPaths);

    data.forEach((key, value) -> {
      try {

        futures.add(overseer.getTaskExecutor().submit(() -> {
          MDCLoggingContext.setNode(zkController.getNodeName());

          try {
            processEntry(key, value, onStart);
          } catch (Exception e) {
            log.error("failed processing collection queue items {}", items, e);
          }
        }));

      } catch (Exception e) {
        log.error("Exception getting queue data", e);

      }
    });

    if (fullPaths.size() > 0) {
      try {
        zkController.getZkClient().delete(fullPaths, true, true);
      } catch (Exception e) {
        log.warn("Failed deleting processed items", e);
      }
    }

    return null;
  }

  private void processEntry(String path, byte[] data, boolean onStart) {
    ZkStateWriter zkWriter = overseer.getZkStateWriter();
    if (zkWriter == null) {
      log.warn("Overseer appears closed");
      throw new AlreadyClosedException();
    }

    try {

      if (data == null) {
        log.error("empty item {}", path);
        return;
      }

      String responsePath = Overseer.OVERSEER_COLLECTION_MAP_COMPLETED + "/" + OverseerTaskQueue.RESPONSE_PREFIX + path.substring(path.lastIndexOf('-') + 1);

      final ZkNodeProps message = ZkNodeProps.load(data);
      try {
        String operation = message.getStr(Overseer.QUEUE_OPERATION);

        if (operation == null) {
          log.error("Msg does not have required " + Overseer.QUEUE_OPERATION + ": {}", message);
          return;
        }

        final String asyncId = message.getStr(ASYNC);

        OverseerSolrResponse response;
        if (operation != null && operation.startsWith(CONFIGSETS_ACTION_PREFIX)) {
          response = configMessageHandler.processMessage(message, operation, zkWriter);
        } else {
          response = collMessageHandler.processMessage(message, operation, zkWriter);
        }

        if (log.isDebugEnabled()) log.debug("response {}", response);

        if (response == null) {
          NamedList nl = new NamedList();
          nl.add("success", "true");
          response = new OverseerSolrResponse(nl);
        } else if (response.getResponse().size() == 0) {
          response.getResponse().add("success", "true");
        }

        if (asyncId != null) {

          if (log.isDebugEnabled()) {
            log.debug("Updated completed map for task with zkid:[{}]", asyncId);
          }
          completedMap.put(asyncId, OverseerSolrResponseSerializer.serialize(response), CreateMode.PERSISTENT);

        } else {
          byte[] sdata = OverseerSolrResponseSerializer.serialize(response);
          completedMap.update(path.substring(path.lastIndexOf('-') + 1), sdata);
          log.debug("Completed task:[{}] {} {}", message, response.getResponse(), responsePath);
        }

      } catch (Exception e) {
        log.error("Exception processing entry");
      }

    } catch (Exception e) {
      log.error("Exception processing entry", e);
    }

  }
}
