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
package org.apache.solr.bench.generators;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class StatisticsCollector {

  private final Map<List<Object>, Integer> counts = new HashMap<>();

  private final String label;

  private List<StatisticsEntry> statisticsEntries = null;

  public StatisticsCollector(String label) {
    this.label = label;
  }

  public StatisticsCollector collect(Object... values) {
    ensureAtLeastOneParameter(values);
    List<Object> key = keyFrom(values);
    ensureSameNumberOfValues(key);
    updateCounts(key);
    return this;
  }

  public void printReport() {
    // nocommit
    // NumberRangeHistogram histogram =new NumberRangeHistogram();
    Histogram histogram = new Histogram();
    List<String> report = histogram.formatReport(statisticsEntries());

    System.out.println("report:");

    report.forEach(s -> System.out.println(s));
  }

  private void updateCounts(List<Object> key) {
    int count = counts.computeIfAbsent(key, any -> 0);
    counts.put(key, ++count);
    statisticsEntries = null;
  }

  private void ensureAtLeastOneParameter(Object[] values) {
    if (Arrays.equals(values, new Object[0])) {
      String message =
          String.format("StatisticsCollector[%s] must be called with at least one value", label);
      throw new IllegalArgumentException(message);
    }
  }

  private void ensureSameNumberOfValues(List<Object> keyCandidate) {
    if (counts.isEmpty()) {
      return;
    }
    List<Object> anyKey = counts.keySet().iterator().next();
    if (anyKey.size() != keyCandidate.size()) {
      String message =
          String.format(
              "StatisticsCollector[%s] must always be called with same number of values", label);
      throw new IllegalArgumentException(message);
    }
  }

  private List<Object> keyFrom(Object[] values) {
    if (values != null) {
      return Arrays.asList(values);
    } else {
      return Collections.singletonList(null);
    }
  }

  private StatisticsEntry statisticsEntry(Object[] values) {
    List<Object> key = keyFrom(values);
    return statisticsEntries().stream()
        .filter(entry -> entry.values().equals(key))
        .findFirst()
        .orElse(StatisticsEntry.nullFor(key));
  }

  private StatisticsEntry query(Predicate<List<Object>> query) {
    return statisticsEntries().stream()
        .filter(
            entry -> {
              List<Object> values = entry.values();
              return query.test(values);
            })
        .reduce(StatisticsEntry.NULL, StatisticsEntry::plus);
  }

  public int countAllCollects() {
    return counts.values().stream().mapToInt(aCount -> aCount).sum();
  }

  public Map<List<Object>, Integer> getCounts() {
    return counts;
  }

  public List<StatisticsEntry> statisticsEntries() {
    if (statisticsEntries != null) {
      return statisticsEntries;
    }
    statisticsEntries = calculateStatistics();
    return statisticsEntries;
  }

  private List<StatisticsEntry> calculateStatistics() {
    int sum = countAllCollects();
    return counts.entrySet().stream()
        .sorted(this::compareStatisticsEntries)
        .filter(entry -> !entry.getKey().equals(Collections.emptyList()))
        .map(
            entry -> {
              double percentage = entry.getValue() * 100.0 / sum;
              return new StatisticsEntry(
                  entry.getKey(), displayKey(entry.getKey()), entry.getValue(), percentage);
            })
        .collect(Collectors.toList());
  }

  private int compareStatisticsEntries(
      Map.Entry<List<Object>, Integer> e1, Map.Entry<List<Object>, Integer> e2) {
    List<Object> k1 = e1.getKey();
    List<Object> k2 = e2.getKey();
    if (k1.size() != k2.size()) {
      return Integer.compare(k1.size(), k2.size());
    }
    return e2.getValue().compareTo(e1.getValue());
  }

  private String displayKey(List<Object> key) {
    return key.stream().map(Objects::toString).collect(Collectors.joining(" "));
  }
}
