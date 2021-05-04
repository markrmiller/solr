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
package org.apache.lucene.search.grouping;

import org.apache.lucene.util.BytesRef;

/**
 * Represents a facet entry with a value and a count.
 */
public class FacetEntry {

  final BytesRef value;
  final int count;

  public FacetEntry(BytesRef value, int count) {
    this.value = value;
    this.count = count;
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    FacetEntry that = (FacetEntry) o;

    if (count != that.count) return false;
    if (!value.equals(that.value)) return false;

    return true;
  }

  @Override public int hashCode() {
    int result = value.hashCode();
    result = 31 * result + count;
    return result;
  }

  @Override public String toString() {
    return "FacetEntry{" + "value=" + value.utf8ToString() + ", count=" + count + '}';
  }

  /**
   * @return The value of this facet entry
   */
  public BytesRef getValue() {
    return value;
  }

  /**
   * @return The count (number of groups) of this facet entry.
   */
  public int getCount() {
    return count;
  }
}
