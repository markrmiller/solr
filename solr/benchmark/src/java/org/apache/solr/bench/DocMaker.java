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

import static org.apache.solr.bench.BaseBenchState.log;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.SplittableRandom;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.Validate;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.common.util.SuppressForbidden;
import org.quicktheories.core.Gen;
import org.quicktheories.impl.BenchmarkRandomSource;

/**
 * A tool to generate controlled random data for a benchmark. {@link SolrInputDocument}s are created
 * based on supplied FieldDef definitions.
 *
 * <p>You can call getDocument to build and retrieve one {@link SolrInputDocument} at a time, or you
 * can call {@link #preGenerateDocs} to generate the given number of documents in RAM, and then
 * retrieve them via {@link #getGeneratedDocsIterator}.
 */
public class DocMaker {

  private Queue<SolrInputDocument> docs = new ConcurrentLinkedQueue<>();

  private final Map<String, Gen<?>> fields = new HashMap<>();

  private static final AtomicInteger ID = new AtomicInteger();

  private ExecutorService executorService;

  public static DocMaker docs() {
    return new DocMaker();
  }

  private DocMaker() {}

  @SuppressForbidden(reason = "This module does not need to deal with logging context")
  public void preGenerateDocs(int numDocs, SplittableRandom random) throws InterruptedException {
    log("preGenerateDocs " + numDocs + " ...");
    docs.clear();
    executorService =
        Executors.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors() + 1,
            new SolrNamedThreadFactory("SolrJMH DocMaker"));

    for (int i = 0; i < numDocs; i++) {
      executorService.submit(
          new Runnable() {
            SplittableRandom threadRandom = random.split();

            @Override
            public void run() {
              try {
                SolrInputDocument doc = DocMaker.this.getInputDocument(threadRandom);
                docs.add(doc);
              } catch (Exception e) {
                executorService.shutdownNow();
                throw new RuntimeException(e);
              }
            }
          });
    }

    executorService.shutdown();
    boolean result = executorService.awaitTermination(10, TimeUnit.MINUTES);
    if (!result) {
      throw new RuntimeException("Timeout waiting for doc adds to finish");
    }
    log(
        "done preGenerateDocs docs="
            + docs.size()
            + " ram="
            + RamUsageEstimator.humanReadableUnits(RamUsageEstimator.sizeOfObject(docs)));

    if (numDocs != docs.size()) {
      throw new IllegalStateException("numDocs != " + docs.size());
    }
  }

  public Iterator<SolrInputDocument> getGeneratedDocsIterator() {
    return docs.iterator();
  }

  public SolrInputDocument getInputDocument(SplittableRandom random) {
    SolrInputDocument doc = new SolrInputDocument();

    for (Map.Entry<String, Gen<?>> entry : fields.entrySet()) {
      doc.addField(entry.getKey(), entry.getValue().generate(new BenchmarkRandomSource(random)));
    }

    return doc;
  }

  public SolrDocument getDocument(SplittableRandom random) {
    SolrDocument doc = new SolrDocument();

    for (Map.Entry<String, Gen<?>> entry : fields.entrySet()) {
      doc.addField(entry.getKey(), entry.getValue().generate(new BenchmarkRandomSource(random)));
    }

    return doc;
  }

  public DocMaker addField(String name, Gen<?> generator) {
    fields.put(name, generator);
    return this;
  }

  //  public DocMaker addField(String name, FieldDef.FieldDefBuilder fieldDef) {
  //    fields.put(name, new FieldDefValueGenerator(fieldDef.build()));
  //    return this;
  //  }

  public void clear() {
    docs.clear();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DocMaker that = (DocMaker) o;
    return fields.equals(that.fields);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fields);
  }

  public static int nextInt(
      final int startInclusive, final int endExclusive, SplittableRandom random) {
    Validate.isTrue(
        endExclusive >= startInclusive, "Start value must be smaller or equal to end value.");
    Validate.isTrue(startInclusive >= 0, "Both range values must be non-negative.");

    if (startInclusive == endExclusive) {
      return startInclusive;
    }

    return startInclusive + random.nextInt(endExclusive - startInclusive);
  }

  public static long nextLong(
      final long startInclusive, final long endExclusive, SplittableRandom random) {
    Validate.isTrue(
        endExclusive >= startInclusive, "Start value must be smaller or equal to end value.");
    Validate.isTrue(
        startInclusive >= 0,
        "Both range values must be non-negative startInclusive="
            + startInclusive
            + " endExclusive="
            + endExclusive);

    if (startInclusive == endExclusive) {
      return startInclusive;
    }

    return startInclusive + random.nextLong(endExclusive - startInclusive);
  }
}
