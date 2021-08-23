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
package org.quicktheories.impl;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.solr.bench.generators.Distribution;
import org.quicktheories.core.DetatchedRandomnessSource;

public class BenchmarkRandomSource implements ExtendedRandomnessSource {
  RandomGenerator random;
  RandomDataGenerator rdg = new RandomDataGenerator(random);

  private org.apache.solr.bench.generators.Distribution distribution = Distribution.Uniform;

  public BenchmarkRandomSource(RandomGenerator random) {
    this.random = random;
  }

  private BenchmarkRandomSource(RandomGenerator random,  RandomDataGenerator rdg, org.apache.solr.bench.generators.Distribution distribution) {
    this.random = random;
    this.rdg = rdg;
    this.distribution  = distribution;
  }

  public BenchmarkRandomSource withDistribution(org.apache.solr.bench.generators.Distribution distribution) {
    if (this.distribution == distribution) {
      return this;
    }
    return new BenchmarkRandomSource(random, rdg, distribution);
  }

  @Override
  public long next(Constraint constraints) {
    if (constraints.min() == constraints.max()) {
      throw new RuntimeException(constraints.min() + " " + constraints.max());
    }
    switch (distribution) {
      case Uniform:
        return rdg.nextLong(constraints.min(), constraints.max());
      case Zipfian:
        return rdg.nextZipf((int) (constraints.max() - constraints.min()), 2) + constraints.min() - 1;
      case Gaussian:
        return (int) normalize(rdg.nextGaussian(.5, .125), constraints.min(), constraints.max() - 1);
      default:
        throw new IllegalStateException("Unknown distribution: " + distribution);
    }

  }

  private double normalize(double value, double normalizationLowerBound, double normalizationUpperBound) {
    double boundedValue = boundValue(value);
    // normalize boundedValue to new range
    double normalizedRange = normalizationUpperBound - normalizationLowerBound;
    return (((boundedValue - 0) * normalizedRange) / 1) + normalizationLowerBound;
  }

  private double boundValue(double value) {
    double boundedValue = value;
    if (value < 0) {
      boundedValue = 0;
    }
    if (value > 1) {
      boundedValue = 1;
    }
    return boundedValue;
  }

  @Override
  public DetatchedRandomnessSource detach() {
    return new ConcreteDetachedSource(this);
  }

  @Override
  public void registerFailedAssumption() {}

  @Override
  public long tryNext(Constraint constraints) {
    return next(constraints);
  }

  @Override
  public void add(Precursor other) {}
}
