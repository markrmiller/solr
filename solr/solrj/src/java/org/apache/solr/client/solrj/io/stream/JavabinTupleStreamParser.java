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

package org.apache.solr.client.solrj.io.stream;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.solr.common.util.JavaBinCodec;

public class JavabinTupleStreamParser extends JavaBinCodec implements TupleStreamParser {
  private final InputStream is;

  private int arraySize = Integer.MAX_VALUE;
  private boolean onlyJsonTypes = false;
  int objectSize;

  public JavabinTupleStreamParser(InputStream is, boolean onlyJsonTypes) throws IOException {
    this.onlyJsonTypes = onlyJsonTypes;
    this.is = is;
    initRead(is);
    if (!readTillDocs()) arraySize = 0;
  }

  private boolean readTillDocs() throws IOException {
    if (isObjectType()) {
      if (tagByte == SOLRDOCLST) {
        readVal(this); // this is the metadata, throw it away
        tagByte = readByte(this);
        arraySize = readSize(this);
        return true;
      }
      for (int i = objectSize; i > 0; i--) {
        Object k = readVal(this);
        if (k == END_OBJ) break;
        if ("docs".equals(k)) {
          tagByte = readByte(this);
          if (tagByte == ITERATOR) return true; // docs must be an iterator or
          if (tagByte >>> 5 == ARR >>> 5) { // an array
            arraySize = readSize(this);
            return true;
          }
          return false;
        } else {
          if (readTillDocs()) return true;
        }
      }
    } else {
      readObject();
      return false;
    }
    return false;

    // here after it will be a stream of maps
  }

  private boolean isObjectType() throws IOException {
    tagByte = readByte(this);
    if (tagByte >>> 5 == ORDERED_MAP >>> 5 || tagByte >>> 5 == NAMED_LST >>> 5) {
      objectSize = readSize(this);
      return true;
    }
    if (tagByte == MAP) {
      objectSize = readVInt(this);
      return true;
    }
    if (tagByte == MAP_ENTRY_ITER) {
      objectSize = Integer.MAX_VALUE;
      return true;
    }
    return tagByte == SOLRDOCLST;
  }

  private Map<?, ?> readAsMap() throws IOException {
    int sz = readSize(this);
    Map<String, Object> m = new LinkedHashMap<>();
    for (int i = 0; i < sz; i++) {
      String name = (String) readVal(this);
      Object val = readVal(this);
      m.put(name, val);
    }
    return m;
  }

  private Map<?, ?> readSolrDocumentAsMap() throws IOException {
    tagByte = readByte(this);
    int size = readSize(this);
    Map<String, Object> doc = new LinkedHashMap<>();
    for (int i = 0; i < size; i++) {
      String fieldName;
      Object obj = readVal(this); // could be a field name, or a child document
      if (obj instanceof Map) {
        @SuppressWarnings("unchecked")
        List<Object> l = (List<Object>) doc.get("_childDocuments_");
        if (l == null) doc.put("_childDocuments_", l = new ArrayList<>());
        l.add(obj);
        continue;
      } else {
        fieldName = (String) obj;
      }
      Object fieldVal = readVal(this);
      doc.put(fieldName, fieldVal);
    }
    return doc;
  }

  @Override
  protected Object readObject() throws IOException {
    if (tagByte == SOLRDOC) {
      return readSolrDocumentAsMap();
    }
    if (onlyJsonTypes) {
      switch (tagByte >>> 5) {
        case SINT >>> 5:
          int i = readSmallInt(this);
          return (long) i;
        case ORDERED_MAP >>> 5:
        case NAMED_LST >>> 5:
          return readAsMap();
      }

      switch (tagByte) {
        case INT:
          {
            int i = readInt(this);
            return (long) i;
          }
        case FLOAT:
          {
            float v = readFloat();
            return (double) v;
          }
        case BYTE:
          {
            byte b = readByte(this);
            return (long) b;
          }
        case SHORT:
          {
            short s = readShort();
            return (long) s;
          }

        case DATE:
          {
            return Instant.ofEpochMilli(readLong(this)).toString();
          }

        default:
          return super.readObject();
      }
    } else return super.readObject();
  }

  @Override
  @SuppressWarnings({"unchecked"})
  public Map<String, Object> next() throws IOException {
    if (arraySize == 0) return null;
    Object o = readVal(this);
    arraySize--;
    if (o == END_OBJ) return null;
    return (Map<String, Object>) o;
  }

  @Override
  public void close() throws IOException {
    is.close();
  }
}
