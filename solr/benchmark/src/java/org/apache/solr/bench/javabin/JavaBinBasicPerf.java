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

import java.util.Date;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;
import org.apache.solr.bench.DocMaker;
import org.apache.solr.bench.FieldDef;
import org.apache.solr.bench.MiniClusterState;
import org.apache.solr.client.solrj.impl.BinaryRequestWriter;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
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
/** A benchmark for basic JavaBin encode/decode performance . */
public class JavaBinBasicPerf {

  @State(Scope.Benchmark)
  public static class BenchState {

    @Param({"50"})
    public int docCount;

    private byte[] responseByteArray;
    private NamedList response;

    @Setup(Level.Trial)
    public void doSetup() throws Exception {
      SplittableRandom random = new SplittableRandom(MiniClusterState.getRandomSeed());

      response = new NamedList();

      NamedList<Object> header = new NamedList<>();
      header.add("status", 0);
      header.add("headerStuff", "values");
      response.add("header", header);

      DocMaker docMaker = new DocMaker();
      docMaker.addField(
          "id", FieldDef.FieldDefBuilder.aFieldDef().withContent(DocMaker.Content.UNIQUE_INT));

      docMaker.addField(
          "facet_s",
          FieldDef.FieldDefBuilder.aFieldDef()
              .withContent(DocMaker.Content.ALPHEBETIC)
              .withMaxLength(64)
              .withMaxCardinality(5, random));
      docMaker.addField(
          "facet2_s",
          FieldDef.FieldDefBuilder.aFieldDef()
              .withContent(DocMaker.Content.ALPHEBETIC)
              .withMaxLength(16)
              .withMaxCardinality(100, random));
      docMaker.addField(
          "facet3_s",
          FieldDef.FieldDefBuilder.aFieldDef()
              .withContent(DocMaker.Content.UNICODE)
              .withMaxLength(128)
              .withMaxCardinality(1200, random));
      docMaker.addField(
          "text",
          FieldDef.FieldDefBuilder.aFieldDef()
              .withContent(DocMaker.Content.ALPHEBETIC)
              .withMaxLength(800)
              .withMaxTokenCount(random.nextInt(1500)));
      docMaker.addField(
          "text2_s",
          FieldDef.FieldDefBuilder.aFieldDef()
              .withContent(DocMaker.Content.ALPHEBETIC)
              .withMaxLength(800)
              .withMaxTokenCount(random.nextInt(2500)));
      docMaker.addField(
          "text3_t",
          FieldDef.FieldDefBuilder.aFieldDef()
              .withContent(DocMaker.Content.ALPHEBETIC)
              .withMaxLength(800)
              .withMaxTokenCount(random.nextInt(3500)));
      docMaker.addField(
          "int_i", FieldDef.FieldDefBuilder.aFieldDef().withContent(DocMaker.Content.INTEGER));
      docMaker.addField(
          "long1_l", FieldDef.FieldDefBuilder.aFieldDef().withContent(DocMaker.Content.LONG));
      docMaker.addField(
          "long2_l", FieldDef.FieldDefBuilder.aFieldDef().withContent(DocMaker.Content.LONG));
      docMaker.addField(
          "long3_l", FieldDef.FieldDefBuilder.aFieldDef().withContent(DocMaker.Content.LONG));
      docMaker.addField(
          "int2_i",
          FieldDef.FieldDefBuilder.aFieldDef()
              .withContent(DocMaker.Content.INTEGER)
              .withMaxCardinality(500, random));

      SolrDocumentList docList = new SolrDocumentList();
      for (int i = 0; i < docCount; i++) {
        SolrDocument doc = docMaker.getDocument(random);
        docList.add(doc);
      }
      docList.setNumFound(docCount);
      docList.setMaxScore(1.0f);
      docList.setStart(0);

      response.add("docs", docList);

      response.add("int", 42);
      response.add("long", 5000023l);
      response.add("date", new Date());

      try (final JavaBinCodec jbc = new JavaBinCodec()) {
        BinaryRequestWriter.BAOS baos = new BinaryRequestWriter.BAOS(1024 * 256);
        jbc.marshal(response, baos);
        responseByteArray = baos.getbuf();
      }
    }
  }

  @Benchmark
  @Timeout(time = 300)
  public Object encode(BenchState state) throws Exception {
    return Utils.toJavabin(state.response, 1024 * 1024 * 2);
  }

  @Benchmark
  @Timeout(time = 300)
  public Object decode(BenchState state) throws Exception {
    try (JavaBinCodec jbc = new JavaBinCodec()) {
      return jbc.unmarshal(state.responseByteArray);
    }
  }
}
