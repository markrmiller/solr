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

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.solr.common.util.Utils;

public class Replica extends ZkNodeProps {


  /**
   * The replica's state. In general, if the node the replica is hosted on is
   * not under {@code /live_nodes} in ZK, the replica's state should be
   * discarded.
   */
  public enum State {
    
    /**
     * The replica is ready to receive updates and queries.
     * <p>
     * <b>NOTE</b>: when the node the replica is hosted on crashes, the
     * replica's state may remain ACTIVE in ZK. To determine if the replica is
     * truly active, you must also verify that its {@link Replica#getNodeName()
     * node} is under {@code /live_nodes} in ZK (or use
     * {@link ZkStateReader#isNodeLive(String)} (String)}).
     * </p>
     */
    ACTIVE,

    LEADER,
    
    /**
     * The first state before {@link State#RECOVERING}. A node in this state
     * should be actively trying to move to {@link State#RECOVERING}.
     * <p>
     * <b>NOTE</b>: a replica's state may appear DOWN in ZK also when the node
     * it's hosted on gracefully shuts down. This is a best effort though, and
     * should not be relied on.
     * </p>
     */
    DOWN,
    
    /**
     * The node is recovering from the leader. This might involve peer-sync,
     * full replication or finding out things are already in sync.
     */
    RECOVERING,

    BUFFERING,
    
    /**
     * Recovery attempts have not worked, something is not right.
     * <p>
     * <b>NOTE</b>: This state doesn't matter if the node is not part of
     * {@code /live_nodes} in ZK; in that case the node is not part of the
     * cluster and it's state should be discarded.
     * </p>
     */
    RECOVERY_FAILED;
    
    @Override
    public String toString() {
      return super.toString().toLowerCase(Locale.ROOT);
    }

    public static Integer getShortState(State state) {
      switch (state) {
        case LEADER:
          return 1;
        case ACTIVE:
          return 2;
        case RECOVERING:
          return 4;
        case BUFFERING:
          return 3;
        case RECOVERY_FAILED:
          return 6;
        case DOWN:
          return 5;
        default:
          throw new IllegalStateException();
      }
    }

    public static State shortStateToState(Integer shortState) {
      return shortStateToState(shortState, false);
    }

    public static State shortStateToState(Integer shortState, boolean published) {
      if (shortState.equals(1)) {
        if (published) {
          return State.ACTIVE;
        } else {
          return State.LEADER;
        }
      } if (shortState.equals(2)) {
        return State.ACTIVE;
      } if (shortState.equals(4)) {
        return State.RECOVERING;
      } else if (shortState.equals(3)) {
        return State.BUFFERING;
      } else if (shortState.equals(5)) {
        return State.DOWN;
      } else if (shortState.equals(6)) {
        return State.RECOVERY_FAILED;
      }
      throw new IllegalStateException("Unknown state: " + shortState);
    }

    public static Character shortStateToLetterState(Integer shortState) {
      if (shortState.equals(2)) {
        return 'A';
      } if (shortState.equals(1)) {
        return 'L';
      } else if (shortState.equals(4)) {
        return 'R';
      } else if (shortState.equals(3)) {
        return 'B';
      } else if (shortState.equals(5)) {
        return 'D';
      } else if (shortState.equals(6)) {
        return 'R';
      } else if (shortState.equals(0)) {
        return 'U';
      }
      throw new IllegalStateException("Unknown state: " + shortState);
    }


    /** Converts the state string to a State instance. */
    public static State getState(String stateStr) {
      return stateStr == null ? null : State.valueOf(stateStr.toUpperCase(Locale.ROOT));
    }
  }

  public enum Type {
    /**
     * Writes updates to transaction log and indexes locally. Replicas of type {@link Type#NRT} support NRT (soft commits) and RTG. 
     * Any {@link Type#NRT} replica can become a leader. A shard leader will forward updates to all active {@link Type#NRT} and
     * {@link Type#TLOG} replicas. 
     */
    NRT,
    /**
     * Writes to transaction log, but not to index, uses replication. Any {@link Type#TLOG} replica can become leader (by first
     * applying all local transaction log elements). If a replica is of type {@link Type#TLOG} but is also the leader, it will behave 
     * as a {@link Type#NRT}. A shard leader will forward updates to all active {@link Type#NRT} and {@link Type#TLOG} replicas.
     */
    TLOG,
    /**
     * Doesn’t index or writes to transaction log. Just replicates from {@link Type#NRT} or {@link Type#TLOG} replicas. {@link Type#PULL}
     * replicas can’t become shard leaders (i.e., if there are only pull replicas in the collection at some point, updates will fail
     * same as if there is no leaders, queries continue to work), so they don’t even participate in elections.
     */
    PULL;

    public static Type get(String name){
      return name == null ? Type.NRT : Type.valueOf(name.toUpperCase(Locale.ROOT));
    }
  }

  public interface NodeNameToBaseUrl {
    String getBaseUrlForNodeName(final String nodeName);
  }

  private final String name;
  private final String nodeName;
  private volatile AtomicInteger state;

  private final Type type;
  public volatile Slice slice;
  public final String collection;
  private final String sliceName;

  public Replica(String name, Map newProps, String collection, Integer collectionId, String sliceName) {
    this(name, newProps , collection, collectionId, sliceName,null);
  }


  public Replica(String name, Map<String,Object> propMap, String collection, Integer collectionId, String sliceName, Slice slice) {
    super(propMap);
    this.collection = collection;
    this.slice = slice;
    this.sliceName = sliceName;
    this.name = name;

    propMap.remove(ZkStateReader.STATE_PROP);

    this.nodeName = (String) propMap.get(ZkStateReader.NODE_NAME_PROP);

    Object rawId = propMap.get("id");
    if (rawId instanceof Integer) {
      this.id = (Integer) rawId;
    } else {
      this.id = ((Long) rawId).intValue();
    }


    this.collectionId = collectionId;
    if (this.collectionId == null) {
      Object collId = propMap.get("collId");
      if (collId != null) {
        this.collectionId = Integer.parseInt((String) collId);
      }
    } else {
      propMap.put("collId", collectionId);
    }

    type = Type.get((String) propMap.get(ZkStateReader.REPLICA_TYPE));
    Objects.requireNonNull(this.collection, "'collection' must not be null");
    Objects.requireNonNull(this.name, "'name' must not be null");
    Objects.requireNonNull(this.nodeName, "'node_name' must not be null");
    Objects.requireNonNull(this.type, "'type' must not be null");
    Objects.requireNonNull(this.collectionId, "'collectionId' must not be null");


    hashcode = Objects.hash(name, id, collectionId);
  }

  Integer id;
  Integer collectionId;

  private final int hashcode;

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    Replica replica = (Replica) o;
    if (replica == null) return false;
    return name.equals(replica.name) && id.equals(replica.id) && collectionId.equals(replica.collectionId);
  }

  @Override
  public int hashCode() {
    return hashcode;
  }

  public String getId() {
    return collectionId + "-" + (id == null ? null : id.toString());
  }

  public Integer getInternalId() {
    return id;
  }

  public Integer getCollectionId() {
    return collectionId;
  }


  public String getCollection() {
    return collection;
  }

  public String getSlice() {
    return sliceName;
  }

  public Slice getSliceOwner(){
    return slice;
  }

  /** Also known as coreNodeName. */
  public String getName() {
    return name;
  }

  public String getCoreUrl() {
    return getCoreUrl(getBaseUrl(), name);
  }

  public String getBaseUrl() {
    return Utils.getBaseUrlForNodeName(nodeName, "http"); // MRM TODO: https
  }

  /** The name of the node this replica resides on */
  public String getNodeName() {
    return nodeName;
  }
  
  /** Returns the {@link State} of this replica. */
  public State getState() {
//    if (state == null) {
//      return State.DOWN;
//    }
    return Replica.State.shortStateToState(state.get(), true);
  }

  public State getRawState() {
    Integer rawState = state == null ? null : state.get();
    if (rawState == null) {
      return State.DOWN;
    }
    return Replica.State.shortStateToState(rawState, false);
  }

  public void removeStateUpdates() {
    AtomicInteger currentState = state;
    if (currentState != null) {
      currentState.set(5);
    }
  }

  public void setState(AtomicInteger replicaState) {
    if (replicaState != null) {
      this.state = replicaState;
    }
  }

  void setSlice(Slice slice) {
    this.slice = slice;
    if (state != null && state.get() == State.getShortState(State.LEADER)) {
      slice.setLeader(this);
    }
  }

  public boolean isActive(Set<String> liveNodes) {
    State currentState = State.shortStateToState(state.get(), true);
    return this.nodeName != null && liveNodes.contains(this.nodeName) && (currentState == State.ACTIVE);
  }

  public boolean isActive() {

    State currentState = State.shortStateToState(state.get(), true);
    return currentState == State.ACTIVE;
  }
  
  public Type getType() {
    return this.type;
  }

  public String getProperty(String propertyName) {
    final String propertyKey;
    if (!propertyName.startsWith(ZkStateReader.PROPERTY_PROP_PREFIX)) {
      propertyKey = ZkStateReader.PROPERTY_PROP_PREFIX + propertyName;
    } else {
      propertyKey = propertyName;
    }
    final String propertyValue = getStr(propertyKey);
    return propertyValue;
  }

  public static String getCoreUrl(String baseUrl, String coreName) {
    StringBuilder sb = new StringBuilder(baseUrl.length() + coreName.length() + 1);
    sb.append(baseUrl);
    if (!(!baseUrl.isEmpty() && baseUrl.charAt(baseUrl.length() - 1) == '/')) sb.append("/");
    sb.append(coreName);
    return sb.toString();
  }


  public Replica copyWithProps(Map props) {
    Map newProps = new HashMap(propMap);
    newProps.putAll(props);
    Replica r = new Replica(name, newProps, collection, collectionId, sliceName, slice);
    return r;
  }

  public Replica copyWithProps(Slice slice, Map props) {
    Map newProps = new HashMap(propMap);
    newProps.putAll(props);
    Replica r = new Replica(name, newProps, collection, collectionId, sliceName, slice);
    return r;
  }

  @Override
  public String toString() {
    return name + "(" + getId() + ")" + ':' + Utils.toJSONString(this); // small enough, keep it on one line (i.e. no indent)
  }
}
