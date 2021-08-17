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

import org.quicktheories.core.Gen;
import org.quicktheories.core.RandomnessSource;

public class SolrGen<T> implements Gen<T> {

  private final Gen<T> child;

  SolrGen() {
    child = null;

  }

  SolrGen(Gen<T> child) {
    this.child = child;
  }

  @Override
  public T generate(RandomnessSource in) {
    return child.generate(in);
  }

  public SolrGen<T> tracked(StatisticsCollector collector) {
    return new TrackingGenerator<>(this, collector);
  }

  public static class TrackingGenerator<T> extends SolrGen<T> {
    private final SolrGen<T> gen;
    private final StatisticsCollector collector;

    public TrackingGenerator(SolrGen<T> gen, StatisticsCollector collector) {
      this.gen = gen;
      this.collector = collector;
    }

    @Override
    public T generate(RandomnessSource in) {
      T val = gen.generate(in);
      collector.collect(val);
      return val;
    }
  }
}
