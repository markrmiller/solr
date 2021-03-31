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
package org.apache.solr.common.cloud;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.KeeperException;
import org.noggit.JSONWriter;

/**
 * Immutable state of the cloud. Normally you can get the state by using
 * {@link ZkStateReader#getClusterState()}.
 * @lucene.experimental
 */
public class ClusterState implements JSONWriter.Writable {

  private final Integer znodeVersion;

  private final Map<String,CollectionRef> collectionStates;

  public static ClusterState getRefCS(Map<String,DocCollection> collectionStates, Integer znodeVersion) {
    Map<String,CollectionRef> collRefs = new LinkedHashMap<>(collectionStates.size());
    collectionStates.forEach((key, c) -> collRefs.put(key, new CollectionRef(c)));
    return new ClusterState(collRefs, znodeVersion);
  }

  /**
   * Use this if all the collection states are not readily available and some needs to be lazily loaded
   */
  public ClusterState(Map<String,CollectionRef> collectionStates, Integer znodeVersion) {
    this.znodeVersion = znodeVersion;
    this.collectionStates = Collections.unmodifiableMap(new LinkedHashMap<>(collectionStates));
  }

  public ClusterState(Map<String,CollectionRef> lazyCollectionStates, Map<String,DocCollection> watchedCollectionStates) {
    this.znodeVersion = -1;
    LinkedHashMap<String,CollectionRef> collStates = new LinkedHashMap<>(lazyCollectionStates);
    for (DocCollection coll : watchedCollectionStates.values()) {
      collStates.put(coll.getName(), new CollectionRef(coll));
    }
    this.collectionStates = Collections.unmodifiableMap(collStates);
  }

  /**
   * Returns a new cluster state object modified with the given collection.
   *
   * @param collectionName the name of the modified (or deleted) collection
   * @param collection     the collection object. A null value deletes the collection from the state
   * @return the updated cluster state which preserves the zk node version
   */
  public ClusterState copyWith(String collectionName, DocCollection collection) {
    LinkedHashMap<String,CollectionRef> newStates = new LinkedHashMap<>(collectionStates);
    if (collection == null) {
      newStates.remove(collectionName);
    } else {
      newStates.put(collectionName, new CollectionRef(collection));
    }
    ClusterState result = new ClusterState(new LinkedHashMap<>(newStates), znodeVersion);

    return result;
  }

  /**
   * Returns the zNode version that was used to construct this instance.
   */
  public int getZNodeVersion() {
    return znodeVersion;
  }

  /**
   * Returns true if the specified collection name exists, false otherwise.
   * <p>
   * Implementation note: This method resolves the collection reference by calling
   * {@link CollectionRef#get()} which can make a call to ZooKeeper. This is necessary
   * because the semantics of how collection list is loaded have changed in SOLR-6629.
   */
  public boolean hasCollection(String collectionName) {
    return getCollectionOrNull(collectionName) != null;
  }

  /**
   * Get the named DocCollection object, or throw an exception if it doesn't exist.
   */
  public DocCollection getCollection(String collection) {
    DocCollection coll = getCollectionOrNull(collection);
    if (coll == null) throw new SolrException(ErrorCode.BAD_REQUEST, "Could not find collection : " + collection + " collections=" + collectionStates.keySet());
    return coll;
  }

  public CollectionRef getCollectionRef(String coll) {
    return collectionStates.get(coll);
  }

  /**
   * Returns the corresponding {@link DocCollection} object for the given collection name
   * if such a collection exists. Returns null otherwise.  Equivalent to getCollectionOrNull(collectionName, false)
   */
  public DocCollection getCollectionOrNull(String collectionName) {
    return getCollectionOrNull(collectionName, false);
  }

  /**
   * Returns the corresponding {@link DocCollection} object for the given collection name
   * if such a collection exists. Returns null otherwise.
   *
   * @param collectionName Name of the collection
   * @param allowCached    allow LazyCollectionRefs to use a time-based cached value
   *                       <p>
   *                       Implementation note: This method resolves the collection reference by calling
   *                       {@link CollectionRef#get()} which may make a call to ZooKeeper. This is necessary
   *                       because the semantics of how collection list is loaded have changed in SOLR-6629.
   */
  public DocCollection getCollectionOrNull(String collectionName, boolean allowCached) {
    CollectionRef ref = collectionStates.get(collectionName);
    return ref == null ? null : ref.get(allowCached);
  }

  /**
   * Get a map of collection name vs DocCollection objects
   * <p>
   * Implementation note: This method resolves the collection reference by calling
   * {@link CollectionRef#get()} which can make a call to ZooKeeper. This is necessary
   * because the semantics of how collection list is loaded have changed in SOLR-6629.
   *
   * @return a map of collection name vs DocCollection objectoldDoc.getSlicesMap()
   */
  public Map<String,DocCollection> getCollectionsMap() {
    Map<String,DocCollection> result = new LinkedHashMap<>(collectionStates.size());
    collectionStates.forEach((key, value) -> {
      DocCollection collection = value.get();
      if (collection != null) {
        result.put(key, collection);
      }
    });
    return result;
  }

  public String getShardId(String nodeName, String coreName) {
    return getShardId(null, nodeName, coreName);
  }

  public String getShardId(String collectionName, String nodeName, String coreName) {
    Collection<CollectionRef> states = collectionStates.values();
    if (collectionName != null) {
      CollectionRef c = collectionStates.get(collectionName);
      if (c != null) states = Collections.singletonList(c);
    }

    for (CollectionRef ref : states) {
      DocCollection coll = ref.get();
      if (coll == null) continue;// this collection go removed in between, skip
      for (Slice slice : coll.getSlices()) {
        for (Replica replica : slice.getReplicas()) {
          // TODO: for really large clusters, we could 'index' on this
          String rnodeName = replica.getStr(ZkStateReader.NODE_NAME_PROP);
          String rcore = replica.getStr(ZkStateReader.CORE_NAME_PROP);
          if (nodeName.equals(rnodeName) && coreName.equals(rcore)) {
            return slice.getName();
          }
        }
      }
    }
    return null;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(256);
    sb.append("znodeVersion: ").append(znodeVersion);
    sb.append("\n");
    sb.append("collections:").append(collectionStates);
    return sb.toString();
  }

  /**
   * Create a ClusterState from Json.
   *
   * @param version zk version of the clusterstate.json file (bytes)
   * @param bytes   a byte array of a Json representation of a mapping from collection name to the Json representation of a
   *                {@link DocCollection} as written by {@link #write(JSONWriter)}. It can represent
   *                one or more collections.
   * @return the ClusterState
   */
  public static ClusterState createFromJson(int version, byte[] bytes) {
    if (bytes == null || bytes.length == 0) {
      return new ClusterState(Collections.emptyMap(), version);
    }
    Map<String,Object> stateMap = (Map<String,Object>) Utils.fromJSON(bytes);
    return createFromCollectionMap(version, stateMap);
  }

  public static DocCollection createDocCollectionFromJson(int version, byte[] bytes) {
    if (bytes == null || bytes.length == 0) {
      return null;
    }
    Map<String,Object> stateMap = (Map<String,Object>) Utils.fromJSON(bytes);
    ClusterState cs = createFromCollectionMap(version, stateMap);
    if (cs.getCollectionsMap().size() == 0) {
      return null;
    }
    DocCollection docCollection = cs.getCollectionsMap().values().iterator().next();
    return docCollection;
  }

  public static ClusterState createFromCollectionMap( int version, Map<String,Object> stateMap) {
    Map<String,CollectionRef> collections = new LinkedHashMap<>(stateMap.size());
    stateMap.forEach((collectionName, value) -> {
      DocCollection coll = collectionFromObjects(collectionName, (Map<String,Object>) value, version);
      collections.put(collectionName, new CollectionRef(coll));
    });

    return new ClusterState(collections, -1);
  }

  // TODO move to static DocCollection.loadFromMap
  private static DocCollection collectionFromObjects(String name, Map<String,Object> objs, Integer version) {
    Map<String,Object> props;
    Map<String,Slice> slices;

    Map<String,Object> sliceObjs = (Map<String,Object>) objs.get(DocCollection.SHARDS);
    if (sliceObjs == null) {
      // legacy format from 4.0... there was no separate "shards" level to contain the collection shards.
      slices = Slice.loadAllFromMap(name, (Long) objs.get("id"), objs);
      props = Collections.emptyMap();
    } else {
      slices = Slice.loadAllFromMap(name, (Long) objs.get("id"), sliceObjs);
      props = new LinkedHashMap<>(objs);
      props.remove(DocCollection.SHARDS);
    }

    Object routerObj = props.get(DocCollection.DOC_ROUTER);
    DocRouter router;
    if (routerObj == null) {
      router = DocRouter.DEFAULT;
    } else if (routerObj instanceof String) {
      // back compat with Solr4.4
      router = DocRouter.getDocRouter((String) routerObj);
    } else {
      Map routerProps = (Map) routerObj;
      router = DocRouter.getDocRouter((String) routerProps.get("name"));
    }

    return new DocCollection(name, slices, props, router, version);
  }

  @Override
  public void write(JSONWriter jsonWriter) {
    LinkedHashMap<String,DocCollection> map = new LinkedHashMap<>();
    collectionStates.forEach((key, value) -> {
      map.put(key, value.get());
    });
    jsonWriter.write(map);
  }

  /**
   * The version of clusterstate.json in ZooKeeper.
   *
   * @return null if ClusterState was created for publication, not consumption
   * @deprecated true cluster state spans many ZK nodes, stop depending on the version number of the shared node!
   * will be removed in 8.0
   */
  @Deprecated
  public Integer getZkClusterStateVersion() {
    return znodeVersion;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((znodeVersion == null) ? 0 : znodeVersion.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    ClusterState other = (ClusterState) obj;
    if (znodeVersion == null) {
      if (other.znodeVersion != null) return false;
    } else if (!znodeVersion.equals(other.znodeVersion)) return false;
    return true;
  }

  /**
   * Be aware that this may return collections which may not exist now.
   * You can confirm that this collection exists after verifying
   * CollectionRef.get() != null
   */
  public Map<String,CollectionRef> getCollectionStates() {
    return collectionStates;
  }

  /**
   * Iterate over collections. Unlike {@link #getCollectionStates()} collections passed to the
   * consumer are guaranteed to exist.
   *
   * @param consumer collection consumer.
   */
  public void forEachCollection(Consumer<DocCollection> consumer) {
    collectionStates.forEach((s, collectionRef) -> {
      try {
        DocCollection collection = collectionRef.get();
        if (collection != null) {
          consumer.accept(collection);
        }
      } catch (Exception e) {
        Throwable cause = e.getCause();
        if (e instanceof KeeperException.NoNodeException || (cause != null && cause instanceof KeeperException.NoNodeException)) {
          //don't do anything. This collection does not exist
        } else {
          throw e;
        }
      }
    });

  }

  public long getHighestId() {
    long[] highest = new long[1];
    collectionStates.forEach((name, coll) -> highest[0] = Math.max(highest[0], coll.get().getId()));
    return highest[0];
  }

  public String getCollection(long id) {
    Set<Entry<String,CollectionRef>> entries = collectionStates.entrySet();
    for (Entry<String,CollectionRef> entry : entries) {
      DocCollection coll = entry.getValue().get();
      if (coll != null && coll.getId() == id) {
        return entry.getKey();
      }
    }
    return null;
  }

  public static class CollectionRef {
    protected final AtomicInteger gets = new AtomicInteger();
    private final DocCollection coll;

    public int getCount() {
      return gets.get();
    }

    public CollectionRef(DocCollection coll) {
      this.coll = coll;
    }

    /**
     * Return the DocCollection, always refetching if lazy. Equivalent to get(false)
     *
     * @return The collection state modeled in zookeeper
     */
    public DocCollection get() {
      return get(false);
    }

    /**
     * Return the DocCollection
     *
     * @param allowCached Determines if cached value can be used.  Applies only to LazyCollectionRef.
     * @return The collection state modeled in zookeeper
     */
    public DocCollection get(boolean allowCached) {
      gets.incrementAndGet();
      return coll;
    }

    public boolean isLazilyLoaded() {
      return false;
    }

    @Override
    public String toString() {
      if (coll != null) {
        return coll.toString();
      } else {
        return "null DocCollection ref";
      }
    }
  }
}
