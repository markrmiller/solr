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
package org.apache.solr.bench;

import java.util.Random;
import java.util.SplittableRandom;
import org.apache.solr.common.util.SuppressForbidden;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.BenchmarkParams;

@State(Scope.Benchmark)
public class BaseBenchState {

  private static final long RANDOM_SEED = 6624420638116043983L;

  private SplittableRandom random;

  public static boolean quietLog = Boolean.getBoolean("quietLog");

  @SuppressForbidden(reason = "JMH uses std out for user output")
  public static void log(String value) {
    if (!quietLog) {
      System.out.println((value.equals("") ? "" : "--> ") + value);
    }
  }

  @Setup(Level.Trial)
  public void doSetup(BenchmarkParams benchmarkParams) throws Exception {
    System.setProperty("solr.log.name", benchmarkParams.id());

    Long seed = getRandomSeed();

    this.random = new SplittableRandom(seed);
  }

  public static Long getRandomSeed() {
    Long seed = Long.getLong("solr.bench.seed");

    if (seed == null) {
      seed = RANDOM_SEED;
    }

    log("benchmark random seed: " + seed);

    // set the seed used by hard to reach places
    System.setProperty("randomSeed", Long.toString(new Random(seed).nextLong()));

    return seed;
  }

  public SplittableRandom getRandom() {
    return random;
  }
}
