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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.quicktheories.api.Pair;

/**
 * A specialized type of {@linkplain Histogram} to divide collected numbers into range-based
 * clusters for display in a histogram.
 */
public class NumberRangeHistogram extends Histogram {

  /**
   * Determines the number of buckets into which the full range of collected numbers will be
   * clustered.
   *
   * @return A number greater than 0
   */
  protected int buckets() {
    return 20;
  }

  /**
   * Determines how a range of numbers is being displayed.
   *
   * @param min The minimum value of the range (included)
   * @param max The maximum value of the range
   * @param maxIncluded If the maximum value is included in the range
   * @return A string to describe the range
   */
  protected String rangeLabel(BigInteger min, BigInteger max, boolean maxIncluded) {
    return String.format("[%s..%s", min, max) + (maxIncluded ? ']' : '[');
  }

  /** Does not make sense to override since these labels won't be used anyway */
  @Override
  protected final String label(final StatisticsEntry entry) {
    return "not used";
  }

  /** Does not make sense to override since order does not matter for clustering anyway */
  @Override
  protected final Comparator<? super StatisticsEntry> comparator() {
    return (left, right) -> 0;
  }

  /** Does not make sense to override because this has the number range functionality */
  @Override
  protected final List<Bucket> cluster(final List<StatisticsEntry> entries) {
    Pair<BigInteger, BigInteger> minMax = minMax(entries);
    BigInteger min = minMax._1;
    BigInteger max = minMax._2;

    List<Pair<BigInteger, Bucket>> topsAndBuckets = topsAndBuckets(min, max);

    for (StatisticsEntry entry : entries) {
      Bucket bucket = findBucket(topsAndBuckets, value(entry));
      bucket.addCount(entry.count());
    }

    return topsAndBuckets.stream()
        .map(bigIntegerBucketPair -> bigIntegerBucketPair._2)
        .collect(Collectors.toList());
  }

  private Bucket findBucket(List<Pair<BigInteger, Bucket>> topsAndBuckets, BigDecimal value) {
    for (int i = 0; i < topsAndBuckets.size(); i++) {
      Pair<BigInteger, Bucket> topAndBucket = topsAndBuckets.get(i);
      BigInteger top = topAndBucket._1;
      if (value.compareTo(new BigDecimal(top)) < 0) {
        return topAndBucket._2;
      }
      if (i == topsAndBuckets.size() - 1) {
        return topAndBucket._2;
      }
    }
    throw new RuntimeException(String.format("No bucket found for value [%s]", value));
  }

  private List<Pair<BigInteger, Bucket>> topsAndBuckets(
      final BigInteger min, final BigInteger max) {
    BigInteger range = max.subtract(min);
    BigInteger numberOfBuckets = BigInteger.valueOf(buckets());
    BigInteger step = range.divide(numberOfBuckets);
    BigInteger remainder = range.remainder(numberOfBuckets);
    if (remainder.compareTo(BigInteger.ZERO) != 0) {
      step = step.add(BigInteger.ONE);
    }

    List<Pair<BigInteger, Bucket>> topsAndBuckets = new ArrayList<>();
    BigInteger left = min;
    for (BigInteger index = min.add(step); index.compareTo(max) < 0; index = index.add(step)) {
      String label = rangeLabel(left, index, false);
      topsAndBuckets.add(Pair.of(index, new Bucket(label)));
      left = index;
    }
    String label = rangeLabel(left, max, true);
    topsAndBuckets.add(Pair.of(max, new Bucket(label)));
    return topsAndBuckets;
  }

  private Pair<BigInteger, BigInteger> minMax(final List<StatisticsEntry> entries) {
    BigDecimal min = null;
    BigDecimal max = null;

    for (StatisticsEntry entry : entries) {
      try {
        BigDecimal value = value(entry);
        if (min == null || value.compareTo(min) < 0) {
          min = value;
        }
        if (max == null || value.compareTo(max) > 0) {
          max = value;
        }
      } catch (NumberFormatException numberFormatException) {
        String message =
            String.format(
                "NumberRangeHistogram instances only accept numeric values. [%s] is not numeric.",
                entry.values().get(0));
        throw new RuntimeException(message);
      }
    }

    BigInteger maxBigInteger = max.setScale(0, RoundingMode.UP).toBigInteger();
    return Pair.of(min.toBigInteger(), maxBigInteger);
  }

  private BigDecimal value(final StatisticsEntry entry) {
    if (entry.values().size() != 1) {
      String message =
          String.format(
              "NumberRangeHistogram instances only single value. Wrong value: %s.", entry.values());
      throw new RuntimeException(message);
    }
    return new BigDecimal(entry.values().get(0).toString());
  }
}
