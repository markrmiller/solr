package org.apache.solr.jmh;
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
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@BenchmarkMode(Mode.Throughput)
//@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.SECONDS)
@Threads(5)
@Warmup(iterations = 1)
@Measurement(iterations = 1)
public class Bench {


  //@State(Scope.Thread)
  @State(Scope.Benchmark)
  public static class BenchState {

    private static class CollectionCreateAsyncListener implements AsyncListener<NamedList<Object>> {
      @Override public void onSuccess(NamedList<Object> objectNamedList, int code) {

      }

      @Override public void onFailure(Throwable throwable, int code) {
        System.err.println("Collection create call failed! " + throwable);
      }
    }

    @Setup(Level.Trial)
    public void doSetup() throws Exception {
      Path currentRelativePath = Paths.get("");
      String s = currentRelativePath.toAbsolutePath().toString();
      System.out.println("Current relative path is: " + s);
      cluster = new SolrCloudTestCase.Builder(nodeCount, SolrTestUtil.createTempDir()).
          addConfig("conf", Paths.get("src/resources/conf")).formatZk(true).configure();
      client = cluster.getSolrClient().getHttpClient();
      nodes = new ArrayList<>(nodeCount);
      List<JettySolrRunner> jetties = cluster.getJettySolrRunners();
      for (JettySolrRunner runner : jetties) {
        nodes.add(runner.getBaseUrl());
      }
    }

    @TearDown(Level.Trial)
    public void doTearDown() throws Exception {
      cluster.shutdown();
    }

    String collectionName = "testCollection";

    AtomicInteger nameCounter = new AtomicInteger();

    int nodeCount = 3;
    List<String> nodes;
    Random random = new Random();
    MiniSolrCloudCluster cluster;
    Http2SolrClient client;
  }

  @Benchmark
  public void collectionCreate(BenchState state) throws Exception {

    String collectionName = state.collectionName + state.nameCounter.incrementAndGet();
    CollectionAdminRequest.Create request = CollectionAdminRequest.createCollection(collectionName, "conf", 4, 4);
    request.setBasePath(state.nodes.get(state.random.nextInt(state.nodeCount)));

    state.client.asyncRequest(request, null, new BenchState.CollectionCreateAsyncListener());

    state.cluster.waitForActiveCollection(collectionName, 10, TimeUnit.SECONDS, false, 4, 16, true, false);

  }

}