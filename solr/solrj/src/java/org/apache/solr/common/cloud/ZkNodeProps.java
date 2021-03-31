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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.Utils;
import org.noggit.JSONWriter;

import static org.apache.solr.common.util.Utils.toJSONString;

/**
 * ZkNodeProps contains generic immutable properties.
 */
public class ZkNodeProps extends LinkedHashMap implements JSONWriter.Writable {

  /**
   * Construct ZKNodeProps from map.
   */
  public ZkNodeProps(Map<String, Object> propMap) {
    super(propMap);
  }

  public ZkNodeProps plus(String key, java.io.Serializable val) {
    return plus(Collections.singletonMap(key, val));
  }

  public ZkNodeProps plus(Map<String, Object> newVals) {
    ZkNodeProps newZKNodeProps = new ZkNodeProps(newVals);
    newZKNodeProps.putAll(this);
    return newZKNodeProps;
  }

  public ZkNodeProps plus(String... keyVals) {
    ZkNodeProps newZkNodeProps = new ZkNodeProps(this);
    newZkNodeProps.putAll(Utils.makeMap(true, keyVals));
    return newZkNodeProps;
  }

  public ZkNodeProps minus(String... minusKeys) {
    ZkNodeProps props = new ZkNodeProps(this);
    props.keySet().removeAll(Arrays.asList(minusKeys));
    return props;
  }

  /**
   * Constructor that populates the from array of Strings in form key1, value1,
   * key2, value2, ..., keyN, valueN
   */
  public ZkNodeProps(String... keyVals) {
    this(Utils.makeMap(true, (Object[]) keyVals) );
  }

  public static ZkNodeProps fromKeyVals(Object... keyVals)  {
    return new ZkNodeProps(Utils.makeMap(true, keyVals));
  }

  /** Returns a shallow writable copy of the properties */
  public Map<String,Object> shallowCopy() {
    return new LinkedHashMap<>(this);
  }

  /**
   * Create Replica from json string that is typically stored in zookeeper.
   */
  public static ZkNodeProps load(byte[] bytes) {
    Map<String, Object> props = null;
    if (bytes[0] == 2) {
      try (JavaBinCodec jbc = new JavaBinCodec()) {
        props = (Map<String, Object>) jbc.unmarshal(bytes);
      } catch (IOException e) {
        throw new RuntimeException("Unable to parse javabin content");
      }
    } else {
      props = (Map<String, Object>) Utils.fromJSON(bytes);
    }
    return new ZkNodeProps(props);
  }

  @Override
  public void write(JSONWriter jsonWriter) {
    jsonWriter.write(this);
  }
  
  /**
   * Get a string property value.
   */
  public String getStr(String key) {
    Object o = get(key);
    return o == null ? null : o.toString();
  }

  /**
   * Get a string property value.
   */
  public Integer getInt(String key, Integer def) {
    Object o = get(key);
    return o == null ? def : Integer.valueOf(o.toString());
  }

  /**
   * Get a string property value.
   */
  public Long getLong(String key, Long def) {
    Object o = get(key);
    return o == null ? def : Long.valueOf(o.toString());
  }

  /**
   * Get a string property value.
   */
  public String getStr(String key,String def) {
    Object o = get(key);
    return o == null ? def : o.toString();
  }


  @Override
  public String toString() {
    return toJSONString(this);
    /***
    StringBuilder sb = new StringBuilder();
    Set<Entry<String,Object>> entries = propMap.entrySet();
    for(Entry<String,Object> entry : entries) {
      sb.append(entry.getKey() + "=" + entry.getValue() + "\n");
    }
    return sb.toString();
    ***/
  }



  public boolean getBool(String key, boolean b) {
    Object o = get(key);
    if (o == null) return b;
    if (o instanceof Boolean) return (boolean) o;
    return Boolean.parseBoolean(o.toString());
  }

}
