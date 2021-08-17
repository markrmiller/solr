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

import static org.apache.solr.bench.DocMaker.docs;
import static org.apache.solr.bench.generators.SourceDSL.integers;
import static org.apache.solr.bench.generators.SourceDSL.strings;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.SplittableRandom;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.bench.generators.IntegersDSL;
import org.apache.solr.bench.generators.StringsDSL;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.junit.Before;
import org.junit.Test;
import org.quicktheories.impl.BenchmarkRandomSource;

public class DockMakerTest extends SolrTestCaseJ4 {

  @Before
  public void setup() {
    System.setProperty("randomSeed", Long.toString(new Random().nextLong()));
  }

  //  @Test
  //  public void testGenDoc() throws Exception {
  //    SplittableRandom random = new SplittableRandom();
  //
  //    DocMaker docMaker = docs();
  //    docMaker.addField(
  //        "id",
  //        FieldDef.FieldDefBuilder.aFieldDef()
  //            .withContent(FieldDefValueGenerator.Content.UNIQUE_INT));
  //
  //    docMaker.addField(
  //        "facet_s",
  //        FieldDef.FieldDefBuilder.aFieldDef()
  //            .withContent(FieldDefValueGenerator.Content.ALPHEBETIC)
  //            .withMaxLength(64)
  //            .withMaxCardinality(5, random));
  //    docMaker.addField(
  //        "facet2_s",
  //        FieldDef.FieldDefBuilder.aFieldDef()
  //            .withContent(FieldDefValueGenerator.Content.ALPHEBETIC)
  //            .withMaxLength(16)
  //            .withMaxCardinality(100, random));
  //    docMaker.addField(
  //        "facet3_s",
  //        FieldDef.FieldDefBuilder.aFieldDef()
  //            .withContent(FieldDefValueGenerator.Content.UNICODE)
  //            .withMaxLength(128)
  //            .withMaxCardinality(12000, random));
  //    docMaker.addField(
  //        "text",
  //        FieldDef.FieldDefBuilder.aFieldDef()
  //            .withContent(FieldDefValueGenerator.Content.ALPHEBETIC)
  //            .withMaxLength(12)
  //            .withMaxTokenCount(ThreadLocalRandom.current().nextInt(512) + 1));
  //    docMaker.addField(
  //        "int_i",
  //
  // FieldDef.FieldDefBuilder.aFieldDef().withContent(FieldDefValueGenerator.Content.INTEGER));
  //    docMaker.addField(
  //        "int2_i",
  //        FieldDef.FieldDefBuilder.aFieldDef()
  //            .withContent(FieldDefValueGenerator.Content.INTEGER)
  //            .withMaxCardinality(500, random));
  //
  //   // docMaker.addField("zipfian_i", new ZipfianGenerator(100, random));
  //
  //    docMaker.addField("rnd_english_word_s", new EnglishWordGenerator(random));
  //
  //    for (int i = 0; i < 10; i++) {
  //      SolrInputDocument doc = docMaker.getInputDocument(random);
  //      System.out.println("doc:\n" + doc);
  //    }
  //  }

  @Test
  public void testBasicCardinalityAlpha() throws Exception {
    DocMaker docMaker = docs();
    SplittableRandom random = new SplittableRandom();
    int cardinality = 2;

    docMaker.addField(
        "AlphaCard3",
        strings()
            .maxCardinality(
                cardinality,
                strings().alpha().ofLengthBetween(1, 6),
                new BenchmarkRandomSource(random)));

    Set<String> values = new HashSet<>();
    for (int i = 0; i < 10; i++) {
      SolrInputDocument doc = docMaker.getInputDocument(random);
      SolrInputField field = doc.getField("AlphaCard3");
      values.add(field.getValue().toString());
    }
    assertEquals(values.toString(), cardinality, values.size());

    StringsDSL.printReport();

    System.out.println(values);
  }

  //  @Test
  //  public void testBasicCardinalityUnicode() throws Exception {
  //    DocMaker docMaker = docs();
  //    SplittableRandom random = new SplittableRandom();
  //    int cardinality = 4;
  //    docMaker.addField("UnicodeCard3", FieldDef.FieldDefBuilder.aFieldDef()
  //            .withContent(FieldDefValueGenerator.Content.UNICODE)
  //            .withMaxCardinality(cardinality, random));
  //
  //    HashSet<Object> values = new HashSet<>();
  //    for (int i = 0; i < 20; i++) {
  //      SolrInputDocument doc = docMaker.getInputDocument(random);
  //      SolrInputField field = doc.getField("UnicodeCard3");
  //      // System.out.println("field=" + doc);
  //      values.add(field.getValue().toString());
  //    }
  //
  //    assertEquals(values.toString(), cardinality, values.size());
  //  }
  //
  @Test
  public void testBasicCardinalityInteger() throws Exception {
    SplittableRandom random = new SplittableRandom();
    DocMaker docMaker = docs();
    int cardinality = 3;

    docMaker.addField(
        "IntCard2",
        integers()
            .maxCardinality(cardinality, integers().all(), new BenchmarkRandomSource(random)));

    HashSet<Object> values = new HashSet<>();
    for (int i = 0; i < 30; i++) {
      SolrInputDocument doc = docMaker.getInputDocument(random);
      SolrInputField field = doc.getField("IntCard2");
      values.add(field.getValue().toString());
    }
    assertEquals(values.toString(), cardinality, values.size());

    IntegersDSL.printReport();

    System.out.println(values);
  }
}
