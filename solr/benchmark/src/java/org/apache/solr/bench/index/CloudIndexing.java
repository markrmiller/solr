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
package org.apache.solr.bench.index;

import static org.apache.solr.bench.Docs.docs;
import static org.apache.solr.bench.generators.SourceDSL.integers;
import static org.apache.solr.bench.generators.SourceDSL.longs;
import static org.apache.solr.bench.generators.SourceDSL.strings;

import com.sun.management.HotSpotDiagnosticMXBean;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import javax.management.MBeanServer;

import org.apache.solr.bench.MiniClusterState;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.BenchmarkParams;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Threads(4)
@Warmup(time = 15, iterations = 5)
@Measurement(time = 25, iterations = 5)
@Fork(value = 1)
@Timeout(time = 60)
/** A benchmark to experiment with the performance of distributed indexing. */
public class CloudIndexing {

  @State(Scope.Benchmark)
  public static class BenchState {

    String collection = "testCollection";

    @Param("4")
    int nodeCount;

    @Param("5")
    int numShards;

    @Param({"1", "3"})
    int numReplicas;

    @Param({"100000"})
    public int smallDocCount;

    @Param({"50000"})
    public int largeDocCount;

    @Setup(Level.Iteration)
    public void doSetup(MiniClusterState.MiniClusterBenchState miniClusterState) throws Exception {
      System.setProperty("solr.mergePolicyFactory", "org.apache.solr.index.NoMergePolicyFactory");
      miniClusterState.startMiniCluster(nodeCount);
      miniClusterState.createCollection(collection, numShards, numReplicas);
    }

    @TearDown(Level.Iteration)
    public void doTearDown(MiniClusterState.MiniClusterBenchState miniClusterState)
        throws Exception {
      // dumpHeap("/home/markmiller/heap-" + System.nanoTime() + ".hprof", false);
      miniClusterState.shutdownMiniCluster();
    }

    // nocommit
    public static void dumpHeap(String filePath, boolean live) throws IOException {
      MBeanServer server = ManagementFactory.getPlatformMBeanServer();
      HotSpotDiagnosticMXBean mxBean =
          ManagementFactory.newPlatformMXBeanProxy(
              server, "com.sun.management:type=HotSpotDiagnostic", HotSpotDiagnosticMXBean.class);
      mxBean.dumpHeap(filePath, live);
    }

    @State(Scope.Thread)
    public static class Docs {

      private org.apache.solr.bench.Docs largeDocs;
      private Iterator<SolrInputDocument> largeDocIterator;

      private org.apache.solr.bench.Docs smallDocs;
      private Iterator<SolrInputDocument> smallDocIterator;

      @Setup(Level.Trial)
      public void setupDoc(
          BenchmarkParams benchmarkParams,
          BenchState state,
          MiniClusterState.MiniClusterBenchState miniClusterState)
          throws Exception {
        if (benchmarkParams.getBenchmark().endsWith("indexLargeDoc")) {
          largeDocs = docs().field("id", integers().incrementing())
              .field(strings().basicLatinAlphabet().multi(512).ofLengthBetween(1, 64))
              .field(strings().basicLatinAlphabet().multi(512).ofLengthBetween(1, 64))
              .field(integers().all())
              .field(integers().all())
              .field(integers().all())
              .field(longs().all())
              .field(longs().all());

          largeDocIterator = largeDocs.preGenerate(state.largeDocCount);
        } else if (benchmarkParams.getBenchmark().endsWith("indexSmallDoc")) {
          smallDocs = docs().field("id", integers().incrementing())
              .field("text", strings().basicLatinAlphabet().multi(2).ofLengthBetween(1, 32))
              .field("int1_i", integers().all())
              .field("int2_i", integers().all())
              .field("long1_l", longs().all());

          smallDocIterator = smallDocs.preGenerate(state.smallDocCount);
        }
      }

      public SolrInputDocument getLargeDoc() {
        if (!largeDocIterator.hasNext()) {
          largeDocIterator = largeDocs.generatedDocsIterator();
        }
        return largeDocIterator.next();
      }

      public SolrInputDocument getSmallDoc() {
        if (!smallDocIterator.hasNext()) {
          smallDocIterator = smallDocs.generatedDocsIterator();
        }
        return smallDocIterator.next();
      }
    }
  }

  @Benchmark
  @Timeout(time = 300)
  public Object indexLargeDoc(
      MiniClusterState.MiniClusterBenchState miniClusterState,
      BenchState state,
      BenchState.Docs docState)
      throws Exception {
    UpdateRequest updateRequest = new UpdateRequest();
    updateRequest.setBasePath(
        miniClusterState.nodes.get(miniClusterState.getRandom().nextInt(state.nodeCount)));
    SolrInputDocument doc = docState.getLargeDoc();
    updateRequest.add(doc);

    return miniClusterState.client.request(updateRequest, state.collection);
  }

  @Benchmark
  @Timeout(time = 300)
  public Object indexSmallDoc(
      MiniClusterState.MiniClusterBenchState miniClusterState,
      BenchState state,
      BenchState.Docs docState)
      throws Exception {
    UpdateRequest updateRequest = new UpdateRequest();
    updateRequest.setBasePath(
        miniClusterState.nodes.get(miniClusterState.getRandom().nextInt(state.nodeCount)));
    SolrInputDocument doc = docState.getSmallDoc();
    updateRequest.add(doc);

    return miniClusterState.client.request(updateRequest, state.collection);
  }
}
