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
package org.apache.solr.bench.schema;

import org.apache.solr.SolrTestUtil;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.util.AsyncListener;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.util.NamedList;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.SECONDS)
@Threads(1)
@Warmup(iterations = 1)
@Measurement(iterations = 1)
@Fork(jvmArgsAppend = "-Xmx4g")
public class PointsVsTrie {


  //@State(Scope.Thread)
  @State(Scope.Benchmark)
  public static class BenchState {
    String collectionName = "testCollection";

    @Param({"point", "trie"})
    String fieldType;

    int nodeCount = 3;
    List<String> nodes;
    Random random = new Random();
    MiniSolrCloudCluster cluster;
    Http2SolrClient client;

    private static class RequestAsyncListener implements AsyncListener<NamedList<Object>> {
      @Override public void onSuccess(NamedList<Object> objectNamedList, int code) {

      }

      @Override public void onFailure(Throwable throwable, int code) {
        System.err.println("Request call failed! " + throwable);
      }
    }

    @Setup(Level.Trial)
    public void doSetup() throws Exception {
      Path currentRelativePath = Paths.get("");
      String s = currentRelativePath.toAbsolutePath().toString();
      System.out.println("Current relative path is: " + s);

      System.setProperty("solr.tests.dv.as.stored", "false");
      if (fieldType.equals("point")) {
        System.setProperty("solr.tests.IntegerFieldType", "org.apache.solr.schema.IntPointField");
        System.setProperty("solr.tests.FloatFieldType", "org.apache.solr.schema.FloatPointField");
        System.setProperty("solr.tests.LongFieldType", "org.apache.solr.schema.LongPointField");
        System.setProperty("solr.tests.DoubleFieldType", "org.apache.solr.schema.DoublePointField");
        System.setProperty("solr.tests.DateFieldType", "org.apache.solr.schema.DatePointField");
        System.setProperty("solr.tests.numeric.dv", "true");
      } else if (fieldType.equals("trie")) {
        System.setProperty("solr.tests.IntegerFieldType", "org.apache.solr.schema.TrieIntField");
        System.setProperty("solr.tests.FloatFieldType", "org.apache.solr.schema.TrieFloatField");
        System.setProperty("solr.tests.LongFieldType", "org.apache.solr.schema.TrieLongField");
        System.setProperty("solr.tests.DoubleFieldType", "org.apache.solr.schema.TrieDoubleField");
        System.setProperty("solr.tests.DateFieldType", "org.apache.solr.schema.TrieDateField");
        System.setProperty("solr.tests.numeric.dv", "false");
      } else {
        throw new IllegalStateException();
      }

      cluster = new SolrCloudTestCase.Builder(nodeCount, SolrTestUtil.createTempDir()).
          addConfig("conf", Paths.get("solr/test-framework/src/resources/configs/number-fields/conf")).formatZk(true).configure();
      client = cluster.getSolrClient().getHttpClient();
      nodes = new ArrayList<>(nodeCount);
      List<JettySolrRunner> jetties = cluster.getJettySolrRunners();
      for (JettySolrRunner runner : jetties) {
        nodes.add(runner.getBaseUrl());
      }

      CollectionAdminRequest.Create request = CollectionAdminRequest.createCollection(collectionName, "conf", 4, 4);
      request.setBasePath(nodes.get(random.nextInt(nodeCount)));

      client.asyncRequest(request, null, new BenchState.RequestAsyncListener());

      cluster.waitForActiveCollection(collectionName, 5, TimeUnit.SECONDS, false, 4, 16, true, false);
    }

    @TearDown(Level.Trial)
    public void doTearDown() throws Exception {
      cluster.shutdown();
    }

  }

  @Benchmark
  public static void indexNumerics(BenchState state) throws Exception {


  }

  @Benchmark
  public static void indexTries(BenchState state) throws Exception {


  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(PointsVsTrie.class.getSimpleName())
        .build();

    new Runner(opt).run();
  }

}