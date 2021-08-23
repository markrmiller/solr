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
package org.apache.solr.bench.javabin;

import static org.apache.solr.bench.Docs.docs;
import static org.apache.solr.bench.generators.SourceDSL.integers;
import static org.apache.solr.bench.generators.SourceDSL.longs;
import static org.apache.solr.bench.generators.SourceDSL.strings;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;
import org.apache.solr.bench.BaseBenchState;
import org.apache.solr.bench.Docs;
import org.apache.solr.client.solrj.impl.BinaryRequestWriter;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.JavaBinCodec;
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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Threads(1)
@Warmup(time = 15, iterations = 5)
@Measurement(time = 45, iterations = 5)
@Fork(value = 1)
@Timeout(time = 60)
public class JavaBinBasicPerf {

  @State(Scope.Thread)
  public static class ThreadState {
    private final BinaryRequestWriter.BAOS baos = new BinaryRequestWriter.BAOS(1024 * 1024 * 24);
  }

  @State(Scope.Benchmark)
  public static class BenchState {

    @Param({"50"})
    public int count;

    @Param({"default"}) // few_nums, large_strings, many_token_field, child_docs
    public String content;

    private byte[] responseByteArray;
    private Object response;

    @Setup(Level.Trial)
    public void doSetup(BaseBenchState baseBenchState) throws Exception {
      SplittableRandom random = new SplittableRandom(baseBenchState.getRandomSeed());

      if (content.equals("default")) {
        response = defaultContent(count);
      } else if (content.equals("few_nums")) {
        response = fewNumContent(count);
      } else if (content.equals("large_strings")) {
        response = largeStringsContent(count);
      } else if (content.equals("many_token_field")) {
        response = manyTokenFieldContent(count);
      } else if (content.equals("child_docs")) {
        response = childDocsContent(random, count);
      }

      try (final JavaBinCodec jbc = new JavaBinCodec()) {
        BinaryRequestWriter.BAOS baos = new BinaryRequestWriter.BAOS(1024 * 256);
        jbc.marshal(response, baos, true);
        responseByteArray = baos.getbuf();
      }
    }

    private static Object fewNumContent(int count) {
      List<Object> topLevel = new ArrayList<>();
      for (int i = 0; i < count; i++) {
        List<Object> types = new ArrayList<>();

        types.add((short) 2);
        types.add((double) 3);

        types.add(-4);
        types.add(4);
        types.add(42);

        types.add((long) -5);
        types.add((long) 5);
        types.add((long) 50);
        topLevel.add(types);
      }

      return topLevel;
    }

    private static Object defaultContent(int docCount) {
      NamedList<Object> response = new NamedList<>();

      NamedList<Object> header = new NamedList<>();
      header.add("status", 0);
      header.add("headerStuff", "values");
      response.add("header", header);

      Docs docs = docs().field("id", integers().incrementing())
          .field("facet_s", strings().basicLatinAlphabet().maxCardinality(5).ofLengthBetween(1, 64))
          .field("facet2_s", strings().basicLatinAlphabet().maxCardinality(100).ofLengthBetween(1, 16))
          .field("facet3_s", strings().basicLatinAlphabet().maxCardinality(1200).ofLengthBetween(1, 128))
          .field("text", strings().basicLatinAlphabet().multi(800).ofLengthBetween(1, 800))
          .field("text2_s", strings().basicLatinAlphabet().multi(800).ofLengthBetween(1, 2500))
          .field("text3_t", strings().basicLatinAlphabet().multi(800).ofLengthBetween(1, 3500))
          .field("int_i", integers().all())
          .field("long1_l", longs().all())
          .field("long2_l", longs().all()).field("long3_l", longs().all())
          .field("int2_i", integers().allWithMaxCardinality(500));

      SolrDocumentList docList = new SolrDocumentList();
      for (int i = 0; i < docCount; i++) {
        SolrDocument doc = docs.document();
        docList.add(doc);
      }
      docList.setNumFound(docCount);
      docList.setMaxScore(1.0f);
      docList.setStart(0);

      response.add("docs", docList);

      response.add("int", 42);
      response.add("long", 5000_023L);
      response.add("date", new Date());

      return response;
    }

  }

  @Benchmark
  @Timeout(time = 300)
  public Object encode(BenchState state, ThreadState threadState) throws Exception {
    try (final JavaBinCodec jbc = new JavaBinCodec()) {
      jbc.marshal(state.response, threadState.baos, true);
      return threadState.baos;
    } finally {
      threadState.baos.reset();
    }
  }

  @Benchmark
  @Timeout(time = 300)
  public Object decode(BenchState state) throws Exception {
    try (JavaBinCodec jbc = new JavaBinCodec()) {
      return jbc.unmarshal(state.responseByteArray);
    }
  }

  private static Object largeStringsContent(int count) {
    Docs docs =
        docs().field("string_s", strings().basicLatinAlphabet().multi(5500).ofLengthBetween(2000, 2800));

    SolrDocumentList docList = new SolrDocumentList();
    for (int i = 0; i < count; i++) {
      SolrDocument doc = docs.document();
      docList.add(doc);
    }
    docList.setNumFound(count);
    docList.setMaxScore(1.0f);
    docList.setStart(0);

    return docList;
  }

  private static Object manyTokenFieldContent(int count) {
    Docs docs = docs().field("string_s", strings().basicLatinAlphabet().multi(1000).ofLengthBetween(50, 100));
    SolrDocumentList docList = new SolrDocumentList();
    for (int i = 0; i < count; i++) {
      SolrDocument doc = docs.document();
      docList.add(doc);
    }
    docList.setNumFound(count);
    docList.setMaxScore(1.0f);
    docList.setStart(0);

    return docList;
  }

  private static Object childDocsContent(SplittableRandom random, int count) {
    return null; // nocommit
  }
}
