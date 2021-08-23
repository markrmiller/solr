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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.random.Well512a;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.solr.bench.generators.SolrGen;
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
 * can call {@link #preGenerate} to generate the given number of documents in RAM, and then
 * retrieve them via {@link #generatedDocsIterator}.
 */
public class Docs {

  private final ThreadLocal<RandomGenerator> random;
  private final RandomGenerator randomParent;
  private Queue<SolrInputDocument> docs = new ConcurrentLinkedQueue<>();

  private final Map<String, Gen<?>> fields = new HashMap<>();

  private static final AtomicInteger ID = new AtomicInteger();

  private ExecutorService executorService;
  private int stringFields;
  private int multiStringFields;
  private int integerFields;
  private int longFields;

  public static Docs docs() {
    return new Docs();
  }

  public static Docs docs(RandomGenerator random) {
    return new Docs(random);
  }

  private Docs(RandomGenerator random) {
    this.randomParent = random;
    this.random = ThreadLocal.withInitial(() -> new Well512a(random.nextLong())); // TODO: pluggable
  }

  private Docs() {
    this(new Well512a(Long.getLong("randomSeed")));
  }


  @SuppressForbidden(reason = "This module does not need to deal with logging context")
  public Iterator<SolrInputDocument> preGenerate(int numDocs) throws InterruptedException {
    log("preGenerate docs " + numDocs + " ...");
    docs.clear();
    executorService =
        Executors.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors() + 1,
            new SolrNamedThreadFactory("SolrJMH DocMaker"));

    for (int i = 0; i < numDocs; i++) {
      executorService.submit(
          () -> {
            try {
              SolrInputDocument doc = Docs.this.inputDocument();
              docs.add(doc);
            } catch (Exception e) {
              executorService.shutdownNow();
              throw new RuntimeException(e);
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

    return docs.iterator();
  }

  public Iterator<SolrInputDocument> generatedDocsIterator() {
    return docs.iterator();
  }

  public SolrInputDocument inputDocument() {
    SolrInputDocument doc = new SolrInputDocument();

    for (Map.Entry<String, Gen<?>> entry : fields.entrySet()) {
      doc.addField(entry.getKey(), entry.getValue().generate(new BenchmarkRandomSource(random.get())));
    }

    return doc;
  }

  public SolrDocument document() {
    SolrDocument doc = new SolrDocument();

    for (Map.Entry<String, Gen<?>> entry : fields.entrySet()) {
      doc.addField(entry.getKey(), entry.getValue().generate(new BenchmarkRandomSource(random.get())));
    }

    return doc;
  }

  public Docs field(String name, SolrGen<?> generator) {
    fields.put(name, generator);
    return this;
  }

  public Docs field(SolrGen<?> generator) {
    switch (generator.type()) {
      case String:
        fields.put("string" + (stringFields++ > 0 ? stringFields : "") + "_s", generator);
        break;
      case MultiString:
        fields.put("text" + (multiStringFields++ > 0 ? multiStringFields : "") + "_t", generator);
        break;
      case Integer:
        fields.put("int" + (integerFields++ > 0 ? integerFields : "") + "_t", generator);
        break;
      case Long:
        fields.put("long" + (longFields++ > 0 ? longFields : "") + "_t", generator);
        break;
    }

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
    Docs that = (Docs) o;
    return fields.equals(that.fields);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fields);
  }
}
