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

import java.util.SplittableRandom;
import org.quicktheories.core.Gen;
import org.quicktheories.core.RandomnessSource;
import org.quicktheories.generators.Generate;
import org.quicktheories.impl.BenchmarkRandomSource;

public class IntegersDSL {

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
  public SolrGen<Integer> all() {
    return between(Integer.MIN_VALUE, Integer.MAX_VALUE);
  }

  public SolrGen<Integer> all(int maxCardinality) {
    return between(Integer.MIN_VALUE, Integer.MAX_VALUE, maxCardinality);
  }

  /**
   * Generates all possible positive integers in Java, bounded above by Integer.MAX_VALUE.
   *
   * @return a Source of type Integer
   */
  public SolrGen<Integer> allPositive() {
    return between(1, Integer.MAX_VALUE);
  }

  public SolrGen<Integer> allPositive(int maxCardinality) {
    return between(1, Integer.MAX_VALUE, maxCardinality);
  }

  public SolrGen<Integer> incrementing() {
    return new SolrGen<>() {
          int integer = 0;

          @Override
          public Integer generate(RandomnessSource in) {
            return integer++;
          }
        };
  }

  public class IntegerDomainBuilder {

    private final int startInclusive;

    private int maxCardinality;

    private IntegerDomainBuilder(int startInclusive) {
      this.startInclusive = startInclusive;
    }

    public IntegerDomainBuilder maxCardinality(int max) {
      maxCardinality = max;
      return this;
    }

    /**
     * Generates integers within the interval specified with an inclusive lower and upper bound.
     *
     * @param endInclusive - inclusive upper bound of domain
     * @return a Source of type Integer
     */
    public SolrGen<Integer> upToAndIncluding(final int endInclusive) {
      return between(startInclusive, endInclusive, maxCardinality);
    }

    /**
     * Generates integers within the interval specified with an inclusive lower bound and exclusive
     * upper bound.
     *
     * @param endExclusive - exclusive upper bound of domain
     * @return a Source of type Integer
     */
    public SolrGen<Integer> upTo(final int endExclusive) {
      return between(startInclusive, endExclusive - 1, maxCardinality);
    }
  }


  /**
   * Generates Integers within the interval specified with an inclusive lower and upper bound.
   *
   * @param startInclusive - inclusive lower bound of domain
   * @param endInclusive - inclusive upper bound of domain
   * @return a Source of type Integer
   */
  public SolrGen<Integer> between(final int startInclusive, final int endInclusive) {
    return between(startInclusive, endInclusive, 0);
  }

  /**
   * Generates Integers within the interval specified with an inclusive lower and upper bound.
   *
   * @param startInclusive - inclusive lower bound of domain
   * @param endInclusive - inclusive upper bound of domain
   * @return a Source of type Integer
   */
  public SolrGen<Integer> between(final int startInclusive, final int endInclusive, int maxCardinality) {
    checkArguments(
        startInclusive <= endInclusive,
        "There are no Integer values to be generated between (%s) and (%s)",
        startInclusive,
        endInclusive);
    Gen<Integer> integers = Generate.range(startInclusive, endInclusive);
    if (maxCardinality > 0) {
      return new SolrGen<>(new Gen<>() {
        Integer cardinalityStart;

        @Override
        public Integer generate(RandomnessSource in) {
          if (cardinalityStart == null) {
            cardinalityStart = Generate.range(0, Integer.MAX_VALUE - maxCardinality).generate(in);
          }

          long seed = Generate.range(cardinalityStart, cardinalityStart + maxCardinality).generate(in);
          return integers.generate(new BenchmarkRandomSource(new SplittableRandom(seed)));
        }
      });
    } else {
      return new SolrGen<>(integers);
    }
  }
}
