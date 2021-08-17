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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/** A statistics report format to display collected statistics entries as a histogram */
public class Histogram {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static final char BOX = '\u25a0';

  public List<String> formatReport(List<StatisticsEntry> entries) {
    if (entries.isEmpty()) {
      log.warn("No entries for report");
      return Collections.singletonList("No entries for report");
    }
    try {
      entries.sort(comparator());
      List<Bucket> buckets = cluster(entries);
      return generateHistogram(entries, buckets);
    } catch (Exception e) {
      log.warn("Cannot draw histogram", e);
      return Collections.singletonList("Cannot draw histogram: " + e.getMessage());
    }
  }

  /**
   * Determine how many block characters are maximally used to draw the distribution. The more you have the further the
   * histogram extends to the right.
   *
   * <p>Can be overridden.
   *
   * @return A positive number. Default is 80.
   */
  protected int maxDrawRange() {
    return 80;
  }

  /**
   * Determine how entries are being sorted from top to bottom.
   *
   * <p>Can be overridden.
   *
   * @return A comparator instance.
   */
  @SuppressWarnings("unchecked")
  protected Comparator<? super StatisticsEntry> comparator() {
    return (left, right) -> {
      try {
        Comparable<Object> leftFirst = (Comparable<Object>) left.values().get(0);
        Comparable<Object> rightFirst = (Comparable<Object>) right.values().get(0);
        return leftFirst.compareTo(rightFirst);
      } catch (ClassCastException castException) {
        return -Integer.compare(left.count(), right.count());
      }
    };
  }

  /**
   * Determine how entries are being labelled in the histogram.
   *
   * <p>Can be overridden.
   *
   * @param entry
   * @return A non-null string
   */
  protected String label(final StatisticsEntry entry) {
    return entry.name();
  }

  /**
   * Cluster entries into {@linkplain Bucket buckets}.
   *
   * <p>Override if entries should be aggregated into buckets to display in histogram.
   *
   * @param entries An already sorted list of entries
   * @return A sorted list of buckets
   */
  protected List<Bucket> cluster(final List<StatisticsEntry> entries) {
    return entries.stream()
        .map(entry -> new Bucket(label(entry), entry.count()))
        .collect(Collectors.toList());
  }

  /**
   * Change the displayed name of the label column
   *
   * @return the string to show as header of the column
   */
  protected String labelColumnHeader() {
    return "label";
  }

  private List<String> generateHistogram(
      final List<StatisticsEntry> entries, final List<Bucket> buckets) {
    int labelWidth = calculateLabelWidth(buckets);
    int maxCount = buckets.stream().mapToInt(bucket1 -> bucket1.count).max().orElse(0);
    int countWidth = calculateCountWidth(maxCount);
    double scale = Math.max(1.0, maxCount / (double) maxDrawRange());

    List<String> lines = new ArrayList<>();
    String headerFormat = "%1$4s | %2$" + labelWidth + "s | %3$" + countWidth + "s | %4$s";
    String bucketFormat = "%1$4s | %2$" + labelWidth + "s | %3$" + countWidth + "d | %4$s";

    lines.add(header(headerFormat));
    lines.add(ruler(headerFormat));
    for (int i = 0; i < buckets.size(); i++) {
      Bucket bucket = buckets.get(i);
      lines.add(bucketLine(bucketFormat, i, scale, bucket));
    }
    return lines;
  }

  private int calculateCountWidth(final int maxCount) {
    int decimals = (int) Math.max(1, Math.floor(Math.log10(maxCount)) + 1);
    return Math.max(5, decimals);
  }

  private int calculateLabelWidth(final List<Bucket> buckets) {
    int maxLabelLength = buckets.stream().mapToInt(bucket -> bucket.label.length()).max().orElse(0);
    return Math.max(5, maxLabelLength);
  }

  private String bucketLine(String format, int index, final double scale, Bucket bucket) {
    String bars = bars(bucket.count, scale);
    return String.format(format, index, bucket.label, bucket.count, bars);
  }

  private String bars(int num, double scale) {
    int weight = (int) (num / scale);
    return multiply(BOX, weight);
  }

  private String multiply(final char c, final int times) {
    StringBuilder builder = new StringBuilder();
    for (int j = 0; j < times; j++) {
      builder.append(c);
    }
    return builder.toString();
  }

  private String header(String format) {
    return String.format(format, "#", labelColumnHeader(), "count", "");
  }

  private String ruler(String format) {
    String barRuler = multiply('-', maxDrawRange());
    return String.format(format, "", "", "", barRuler).replace(" ", "-");
  }

  public static class Bucket {
    private final String label;
    private int count = 0;

    public Bucket(String label) {
      this(label, 0);
    }

    public Bucket(String label, final int initialCount) {
      if (label == null) {
        throw new IllegalArgumentException("label must not be null");
      }
      this.label = label;
      this.count = initialCount;
    }

    void addCount(int count) {
      this.count += count;
    }
  }
}
