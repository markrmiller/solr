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
import org.quicktheories.impl.BenchmarkRandomSource;

public class SolrGen<T> implements Gen<T> {

  protected final Gen<T> child;
  private final Type type;
  private Distribution distribution = Distribution.Uniform;

  SolrGen() {
    this(Type.Unknown);
  }

  SolrGen(Gen<T> child, Type type) {
    this.child = child;
    if (child instanceof SolrGen) {
      ((SolrGen<Object>) child).distribution = distribution;
    }
    this.type = type;
  }

  SolrGen(Type type) {
    child = null;
    this.type = type;
  }

  SolrGen(Gen<T> child) {
    this(child, Type.Unknown);
  }

  @Override
  public T generate(RandomnessSource in) {
    //System.out.println("gen : " + toString() + " child: " + child.toString());
    if (in instanceof BenchmarkRandomSource) {
      return child.generate(((BenchmarkRandomSource) in).withDistribution(distribution));
    }
    return child.generate(in);
  }

  public Type type() {
    return type;
  }

  public SolrGen<T> tracked(StatCollector collector) {
    return new TrackingGenerator<>(this, collector);
  }

  public SolrGen<T> withDistribution(Distribution distribution) {
    //System.out.println("set dist gen : " + toString() + " child: " + child.toString());
    this.distribution = distribution;
    if (this.child instanceof SolrGen) {
      ((SolrGen<Object>) this.child).distribution = distribution;
    }
    return this;
  }

  @Override
  public String toString() {
    return "SolrGen{" + "child=" + child + ", type=" + type + ", distribution=" + distribution + '}';
  }

  protected Distribution getDistribution() {
    return this.distribution;
  }

  public class TrackingGenerator<T> extends SolrGen<T> {
    private final SolrGen<T> gen;
    private final StatCollector collector;

    public TrackingGenerator(SolrGen<T> gen, StatCollector collector) {
      this.gen = gen;
      this.collector = collector;
    }

    @Override
    public T generate(RandomnessSource in) {
      T val;
      if (in instanceof BenchmarkRandomSource) {
        val = gen.generate(((BenchmarkRandomSource) in).withDistribution(distribution));
        collector.collect(val);
        return val;
      }
      val = gen.generate(in);
      collector.collect(val);
      return val;
    }

    public SolrGen<T> withDistribution(Distribution distribution) {
      if (this.child != null) {
        throw new IllegalStateException();
      }
      gen.withDistribution(distribution);

      return this;
    }

    @Override
    public String toString() {
      return "TrackingSolrGen{" + "wrapped=" + gen + ", type=" + type + ", distribution=" + distribution + '}';
    }
  }
}
