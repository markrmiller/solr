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
package org.apache.solr.bench.search;

import org.apache.solr.bench.DocMakerRamGen;
import org.apache.solr.bench.FieldDef;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Threads(1)
@Warmup(time = 5, iterations = 2)
@Measurement(time = 10, iterations = 3)
// unless fork=0, this jvm jvmArgsPrepend only applies when running via IDE, when using gradle, these come from build.gradle
@Fork(value = 1, jvmArgsPrepend = {"-Dlog4j.configurationFile=../server/resources/log4j2.xml"
        //  "-XX:+FlightRecorder", "-XX:StartFlightRecording=filename=jfr_results/,dumponexit=true,settings=profile,path-to-gc-roots=true"})
})
@Timeout(time = 60)
public class DrillVsFacetExpression {

  private static final String QUERY = "*:*";
  // private static final String QUERY = "int_i:[52 TO 1000000]";

  @State(Scope.Benchmark)
  public static class BenchState {

    String collectionName = "testCollection";

    // @Param({"10000","100000","1000000"})
    @Param({"10000","300000"})
    int docCount;

    int limit = 100;

    boolean countDistinct = false;

    int nodeCount = 4;
    int numShards = 5;

    int numReplicas = 3;

    int facetCardinality = 10000;

    List<String> nodes;
    MiniSolrCloudCluster cluster;
    SolrClient client;

    @Setup(Level.Trial)
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

      DocMakerRamGen docMaker = new DocMakerRamGen();
      docMaker.addField("id", FieldDef.FieldDefBuilder.aFieldDef().withContent(DocMakerRamGen.Content.UNIQUE_INT));
      docMaker.addField("facet_s", FieldDef.FieldDefBuilder.aFieldDef().withContent(DocMakerRamGen.Content.ALPHEBETIC).
          withMaxLength(64).withMaxCardinality(facetCardinality));
      docMaker.addField("int_i", FieldDef.FieldDefBuilder.aFieldDef().withContent(DocMakerRamGen.Content.INTEGER));

      docMaker.preGenerateDocs(docCount);

      Iterator<SolrInputDocument> docIt = docMaker.getGeneratedDocsIterator();

      ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() + 1);
      for (int i = 0; i < docCount; i++) {
        executorService.submit(() -> {
          UpdateRequest updateRequest = new UpdateRequest();
          updateRequest.setBasePath(nodes.get(ThreadLocalRandom.current().nextInt(nodeCount)));
          SolrInputDocument doc = docIt.next();

          updateRequest.add(doc);

          try {
            client.request(updateRequest, collectionName);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
      }

      executorService.shutdown();
      boolean result = executorService.awaitTermination(10, TimeUnit.MINUTES);
      if (!result) {
        throw new RuntimeException("Timeout waiting for doc adds to finish");
      }

      UpdateRequest commitRequest = new UpdateRequest();
      commitRequest.setBasePath(nodes.get(ThreadLocalRandom.current().nextInt(nodeCount)));
      commitRequest.setAction(UpdateRequest.ACTION.COMMIT, false, true);
      commitRequest.process(client, collectionName);

      // we control segment count for a more informative benchmark *and* because background merging would continue after
      // indexing and overlap with the benchmark (does this work as expected?)
      UpdateRequest optimizeRequest = new UpdateRequest();
      optimizeRequest.setBasePath(nodes.get(ThreadLocalRandom.current().nextInt(nodeCount)));
      optimizeRequest.setAction(UpdateRequest.ACTION.OPTIMIZE, false, true, 5);
      optimizeRequest.process(client, collectionName);

    }


    @TearDown(Level.Trial)
    public void doTearDown() throws Exception {
      IOUtils.closeQuietly(client);
      cluster.shutdown();
    }

  }

  // Note: drill must have a single sort specified and a fl

  @Benchmark
  @Timeout(time = 300)
  public static void drill(BenchState state) throws Exception {
    // can we somehow specify a bucket limit?

    ModifiableSolrParams params = new ModifiableSolrParams();
    String expr;

    if (state.countDistinct) {
      expr = "rollup(select(drill("
          + state.collectionName + ", "
          + "q=\"" + QUERY + "\", "
          + "fl=\"facet_s, int_i\", "
          + "sort=\"facet_s desc\", "
//        + "rows=1, "
          + "rollup(input(), over=\"facet_s\", countDist(facet_s), sum(int_i)))," +
          "facet_s, countDist(facet_s) as cnt, sum(int_i) as sum_int)," +
          "over=\"facet_s\"," +
          "countDist(cnt), sum(sum_int)" // you likely cannot do this?
          + ")";
    } else {
      expr = "rollup(select(drill("
          + state.collectionName + ", "
          + "q=\"" + QUERY + "\", "
          + "fl=\"facet_s, int_i\", "
          + "sort=\"facet_s desc\", "
//        + "rows=1, "
          + "rollup(input(), over=\"facet_s\", count(*), sum(int_i)))," +
          "facet_s, count(*) as cnt, sum(int_i) as sum_int)," +
          "over=\"facet_s\"," +
          "sum(cnt), sum(sum_int)"
          + ")";
    }

    params.set("expr", expr);
    params.set("qt", "/stream");

    QueryRequest queryRequest = new QueryRequest(params);
    queryRequest.setBasePath(state.nodes.get(ThreadLocalRandom.current().nextInt(state.nodeCount)));


    NamedList<Object> result = state.client.request(queryRequest, state.collectionName);
    // System.out.println("result: " + result);
  }

  @Benchmark
  @Timeout(time = 300)
  public static void facetExp(BenchState state) throws Exception {

    ModifiableSolrParams params = new ModifiableSolrParams();
    String expr = "facet("
        + state.collectionName + ", "
        + "q=\"" + QUERY + "\", "
        + "fl=\"facet_s, int_i\", "
        + "sort=\"facet_s desc\", "
        + "buckets=\"facet_s\","
        + "bucketSizeLimit=" + state.limit + ","
//        + "           rows=1,"
        + "sum(int_i),"
        + (state.countDistinct ? "countDistinct(facet_s)" : "count(*)")
        + ")";
    params.set("expr", expr);
    params.set("qt", "/stream");

    QueryRequest queryRequest = new QueryRequest(params);
    queryRequest.setBasePath(state.nodes.get(ThreadLocalRandom.current().nextInt(state.nodeCount)));


    NamedList<Object> result = state.client.request(queryRequest, state.collectionName);
    // System.out.println("result: " + result);
  }

  @Benchmark
  @Timeout(time = 300)
  public static void facet(BenchState state) throws Exception {

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("q", QUERY);
    params.set("rows", "0");

    // not per bucket
    // params.set("json.facet", "{ x : \"unique(facet_s)\", y : \"sum(int_i)\"  }");

    // per bucket
    if (state.countDistinct) {
      params.set("json.facet", "{ facets: { type: terms, limit: " + state.limit + ", field: facet_s, facet: {x : \"unique(facet_s)\", y : \"sum(int_i)\" }}}");
    } else {
      params.set("json.facet", "{ facets: { type: terms, limit: " + state.limit + ", field: facet_s, facet: {y : \"sum(int_i)\" }}}");
    }

    QueryRequest queryRequest = new QueryRequest(params);
    queryRequest.setBasePath(state.nodes.get(ThreadLocalRandom.current().nextInt(state.nodeCount)));


    NamedList<Object> result = state.client.request(queryRequest, state.collectionName);

    // System.out.println("result: " + result);
  }

  @Benchmark
  @Timeout(time = 300)
  // uses HyperLogLog if running with countDistinct
  public static void facetHLLUnique(BenchState state) throws Exception {

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("q", QUERY);
    params.set("rows", "0");

    // not per bucket
    // params.set("json.facet", "{ x : \"hll(facet_s)\", y : \"sum(int_i)\" }");

    // per bucket
    if (state.countDistinct) {
      params.set("json.facet", "{ facets: { type: terms, limit: " + state.limit + ", field: facet_s, facet: {x : \"hll(facet_s)\", y : \"sum(int_i)\" }}}");
    } else {
      params.set("json.facet", "{ facets: { type: terms, limit: " + state.limit + ", field: facet_s, facet: {y : \"sum(int_i)\" }}}");
    }

    QueryRequest queryRequest = new QueryRequest(params);
    queryRequest.setBasePath(state.nodes.get(ThreadLocalRandom.current().nextInt(state.nodeCount)));


    NamedList<Object> result = state.client.request(queryRequest, state.collectionName);

    // System.out.println("result: " + result);
  }

}