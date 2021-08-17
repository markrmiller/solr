package org.quicktheories.impl;

import org.HdrHistogram.Histogram;
import org.quicktheories.core.Gen;
import org.quicktheories.core.RandomnessSource;

public class TrackingGenerator<T> implements Gen<T> {
  private final Gen<T> gen;
  private final StatisticsCollector histogram;

  public TrackingGenerator(Gen<T> gen, StatisticsCollector histogram) {
	    this.gen = gen;
	    this.histogram = histogram;
  }

  @Override
  public T generate(RandomnessSource in) {
    T val = gen.generate(in);
    histogram.collect(val);
    return val;
  }
}
