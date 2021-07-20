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

import org.apache.solr.bench.DocMakerRamGen;
import org.apache.solr.bench.FieldDef;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.IOUtils;
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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Threads(6)
@Warmup(time = 5, iterations = 2)
@Measurement(time = 10, iterations = 3)
// unless fork=0, this jvm jvmArgsPrepend only applies when running via IDE, when using gradle, these come from build.gradle
@Fork(value = 1, jvmArgsPrepend = {"-Dlog4j.configurationFile=../server/resources/log4j2.xml"
        //  "-XX:+FlightRecorder", "-XX:StartFlightRecording=filename=jfr_results/,dumponexit=true,settings=profile,path-to-gc-roots=true"})
})
@Timeout(time = 60)
public class CloudIndexing {

  @State(Scope.Benchmark)
  public static class BenchState {

    String collectionName = "testCollection";

    int nodeCount = 4;
    int numShards = 5;

    @Param({"1", "3", "9"})
    int numReplicas;

    List<String> nodes;
    MiniSolrCloudCluster cluster;
    SolrClient client;

    @Setup(Level.Iteration)
    public void doSetup() throws Exception {

      Long seed = Long.getLong("solr.bench.seed");

      if (seed == null) {
        seed = ThreadLocalRandom.current().nextLong();
      }

      // set the seed used by ThreadLocalRandom
      System.setProperty("threadLocalRandomSeed", Long.toString(new Random(seed).nextLong()));

      System.setProperty("pkiHandlerPrivateKeyPath", "");
      System.setProperty("pkiHandlerPublicKeyPath", "");

      Path currentRelativePath = Paths.get("");
      String s = currentRelativePath.toAbsolutePath().toString();

      System.out.println("\n\nCurrent relative path is: " + s);

      // not currently usable, but would enable JettySolrRunner's ill-conceived jetty.testMode and allow using SSL

      // System.getProperty("jetty.testMode", "true");
      // SolrCloudTestCase.sslConfig = SolrTestCaseJ4.buildSSLConfig();

      cluster = new MiniSolrCloudCluster.Builder(nodeCount, Files.createTempDirectory("solr-bench")).
          addConfig("conf", Paths.get("src/resources/configs/cloud-minimal/conf")).configure();
      System.out.println("cluster base path=" + cluster.getBaseDir());

      nodes = new ArrayList<>(nodeCount);
      List<JettySolrRunner> jetties = cluster.getJettySolrRunners();
      for (JettySolrRunner runner : jetties) {
        nodes.add(runner.getBaseUrl().toString());
      }

      client = new Http2SolrClient.Builder().build();

      CollectionAdminRequest.Create request = CollectionAdminRequest.createCollection(collectionName, "conf", numShards, numReplicas);
      request.setBasePath(nodes.get(ThreadLocalRandom.current().nextInt(nodeCount)));

      client.request(request);

      cluster.waitForActiveCollection(collectionName, 15, TimeUnit.SECONDS, numShards, numShards * numReplicas);
    }

    @State(Scope.Thread)
    public static class Docs {
      private final int numDocs = 5000;

      private DocMakerRamGen largeDocMaker;
      private Iterator<SolrInputDocument> largeDocIterator;

      private DocMakerRamGen smallDocMaker;
      private Iterator<SolrInputDocument> smallDocIterator;

      @Setup(Level.Trial)
      public void setupDoc(BenchState state) throws Exception {
        largeDocMaker = new DocMakerRamGen();
        largeDocMaker.addField("id", FieldDef.FieldDefBuilder.aFieldDef().withContent(DocMakerRamGen.Content.UNIQUE_INT));
        largeDocMaker.addField("text_t", FieldDef.FieldDefBuilder.aFieldDef().withContent(DocMakerRamGen.Content.ALPHEBETIC).
            withMaxLength(64).withTokenCount(ThreadLocalRandom.current().nextInt(512) + 1));

        largeDocMaker.preGenerateDocs(numDocs);

        largeDocIterator = largeDocMaker.getGeneratedDocsIterator();

        smallDocMaker = new DocMakerRamGen();
        smallDocMaker.addField("id", FieldDef.FieldDefBuilder.aFieldDef().withContent(DocMakerRamGen.Content.UNIQUE_INT));
        smallDocMaker.addField("text_t", FieldDef.FieldDefBuilder.aFieldDef().withContent(DocMakerRamGen.Content.ALPHEBETIC).
            withMaxLength(32).withTokenCount(1));

        smallDocMaker.preGenerateDocs(numDocs);

        smallDocIterator = smallDocMaker.getGeneratedDocsIterator();
      }

      public SolrInputDocument getLargeDoc() {
        if (!largeDocIterator.hasNext()) {
          largeDocIterator = largeDocMaker.getGeneratedDocsIterator();
        }
        return largeDocIterator.next();
      }

      public SolrInputDocument getSmallDoc() {
        if (!smallDocIterator.hasNext()) {
          smallDocIterator = smallDocMaker.getGeneratedDocsIterator();
        }
        return smallDocIterator.next();
      }
    }

    @TearDown(Level.Iteration)
    public void doTearDown() throws Exception {
      IOUtils.closeQuietly(client);
      cluster.shutdown();
    }

  }

  @Benchmark
  @Timeout(time = 300)
  public static void indexLargeDoc(BenchState state, BenchState.Docs docState) throws Exception {
    UpdateRequest updateRequest = new UpdateRequest();
    updateRequest.setBasePath(state.nodes.get(ThreadLocalRandom.current().nextInt(state.nodeCount)));
    SolrInputDocument doc = docState.getLargeDoc();

    updateRequest.add(doc);

    state.client.request(updateRequest, state.collectionName);
  }

  @Benchmark
  @Timeout(time = 300)
  public static void indexSmallDoc(BenchState state, BenchState.Docs docState) throws Exception {
    UpdateRequest updateRequest = new UpdateRequest();
    updateRequest.setBasePath(state.nodes.get(ThreadLocalRandom.current().nextInt(state.nodeCount)));
    SolrInputDocument doc = docState.getSmallDoc();

    updateRequest.add(doc);

    state.client.request(updateRequest, state.collectionName);
  }
}