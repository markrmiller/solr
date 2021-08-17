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

import static org.apache.solr.bench.generators.SourceDSL.checkArguments;

import java.util.List;
import java.util.SplittableRandom;
import org.quicktheories.core.Gen;
import org.quicktheories.core.RandomnessSource;
import org.quicktheories.generators.Generate;
import org.quicktheories.impl.BenchmarkRandomSource;

public class IntegersDSL {

  private static final boolean  TRACK = false;

  static StatisticsCollector collector = new StatisticsCollector("Label");

  private Gen<Integer> maybeTrack(Gen<Integer> gen) {
    if (!TRACK) return gen;
    return new TrackingGenerator<>(gen, collector);
  }

  public static void printReport() {
    // NumberRangeHistogram histogram =new NumberRangeHistogram();
    Histogram histogram = new Histogram();
    List<String> report = histogram.formatReport(collector.statisticsEntries());

    System.out.println("report:");

    report.forEach(s -> System.out.println(s));
  }

  public Gen<Integer> maxCardinality(int max, Gen<Integer> integers, RandomnessSource random) {
    return maybeTrack(
        new Gen<Integer>() {
          int cardinalityStart = Generate.range(0, Integer.MAX_VALUE - max).generate(random);

          @Override
          public Integer generate(RandomnessSource in) {
            long seed = Generate.range(cardinalityStart, cardinalityStart + max).generate(random);
            return integers.generate(new BenchmarkRandomSource(new SplittableRandom(seed)));
          }
        });
  }

  /**
   * Constructs a IntegerDomainBuilder object with an inclusive lower bound
   *
   * @param startInclusive - lower bound of domain
   * @return an IntegerDomainBuilder
   */
  public IntegerDomainBuilder from(final int startInclusive) {
    return new IntegerDomainBuilder(startInclusive);
  }

  /**
   * Generates all possible integers in Java bounded below by Integer.MIN_VALUE and above by
   * Integer.MAX_VALUE.
   *
   * @return a Source of type Integer
   */
  public Gen<Integer> all() {
    return maybeTrack(between(Integer.MIN_VALUE, Integer.MAX_VALUE));
  }

  /**
   * Generates all possible positive integers in Java, bounded above by Integer.MAX_VALUE.
   *
   * @return a Source of type Integer
   */
  public Gen<Integer> allPositive() {
    return between(1, Integer.MAX_VALUE);
  }

  public Gen<Integer> incrementing() {
    return maybeTrack(
        new Gen<Integer>() {
          int integer = 0;

          @Override
          public Integer generate(RandomnessSource in) {
            return integer++;
          }
        });
  }

  public class IntegerDomainBuilder {

    private final int startInclusive;

    private IntegerDomainBuilder(int startInclusive) {
      this.startInclusive = startInclusive;
    }

    /**
     * Generates integers within the interval specified with an inclusive lower and upper bound.
     *
     * @param endInclusive - inclusive upper bound of domain
     * @return a Source of type Integer
     */
    public Gen<Integer> upToAndIncluding(final int endInclusive) {
      return between(startInclusive, endInclusive);
    }

    /**
     * Generates integers within the interval specified with an inclusive lower bound and exclusive
     * upper bound.
     *
     * @param endExclusive - exclusive upper bound of domain
     * @return a Source of type Integer
     */
    public Gen<Integer> upTo(final int endExclusive) {
      return between(startInclusive, endExclusive - 1);
    }
  }

  /**
   * Generates Integers within the interval specified with an inclusive lower and upper bound.
   *
   * @param startInclusive - inclusive lower bound of domain
   * @param endInclusive - inclusive upper bound of domain
   * @return a Source of type Integer
   */
  public Gen<Integer> between(final int startInclusive, final int endInclusive) {
    checkArguments(
        startInclusive <= endInclusive,
        "There are no Integer values to be generated between (%s) and (%s)",
        startInclusive,
        endInclusive);
    return maybeTrack(Generate.range(startInclusive, endInclusive));
  }
}
