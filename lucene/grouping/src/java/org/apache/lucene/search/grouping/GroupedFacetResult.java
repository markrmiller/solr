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

import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

/**
 * The grouped facet result. Containing grouped facet entries, total count and total missing count.
 */
public class GroupedFacetResult {

  private final static Comparator<FacetEntry> orderByCountAndValue = new Comparator<FacetEntry>() {

    @Override public int compare(FacetEntry a, FacetEntry b) {
      int cmp = b.count - a.count; // Highest count first!
      if (cmp != 0) {
        return cmp;
      }
      return a.value.compareTo(b.value);
    }

  };

  private final static Comparator<FacetEntry> orderByValue = new Comparator<FacetEntry>() {

    @Override public int compare(FacetEntry a, FacetEntry b) {
      return a.value.compareTo(b.value);
    }

  };

  private final int maxSize;
  private final NavigableSet<FacetEntry> facetEntries;
  private final int totalMissingCount;
  private final int totalCount;

  private int currentMin;

  public GroupedFacetResult(int size, int minCount, boolean orderByCount, int totalCount, int totalMissingCount) {
    this.facetEntries = new TreeSet<>(orderByCount ? orderByCountAndValue : orderByValue);
    this.totalMissingCount = totalMissingCount;
    this.totalCount = totalCount;
    maxSize = size;
    currentMin = minCount;
  }

  public void addFacetCount(BytesRef facetValue, int count) {
    if (count < currentMin) {
      return;
    }

    FacetEntry facetEntry = new FacetEntry(facetValue, count);
    if (facetEntries.size() == maxSize) {
      if (facetEntries.higher(facetEntry) == null) {
        return;
      }
      facetEntries.pollLast();
    }
    facetEntries.add(facetEntry);

    if (facetEntries.size() == maxSize) {
      currentMin = facetEntries.last().count;
    }
  }

  /**
   * Returns a list of facet entries to be rendered based on the specified offset and limit.
   * The facet entries are retrieved from the facet entries collected during merging.
   *
   * @param offset The offset in the collected facet entries during merging
   * @param limit  The number of facets to return starting from the offset.
   * @return a list of facet entries to be rendered based on the specified offset and limit
   */
  public List<FacetEntry> getFacetEntries(int offset, int limit) {
    List<FacetEntry> entries = new LinkedList<>();

    int skipped = 0;
    int included = 0;
    for (FacetEntry facetEntry : facetEntries) {
      if (skipped < offset) {
        skipped++;
        continue;
      }
      if (included++ >= limit) {
        break;
      }
      entries.add(facetEntry);
    }
    return entries;
  }

  /**
   * Returns the sum of all facet entries counts.
   *
   * @return the sum of all facet entries counts
   */
  public int getTotalCount() {
    return totalCount;
  }

  /**
   * Returns the number of groups that didn't have a facet value.
   *
   * @return the number of groups that didn't have a facet value
   */
  public int getTotalMissingCount() {
    return totalMissingCount;
  }
}
