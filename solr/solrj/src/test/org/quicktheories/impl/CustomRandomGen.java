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
package org.quicktheories.impl;

import org.HdrHistogram.Histogram;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.commons.math3.random.Well512a;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.quicktheories.api.Pair;
import org.quicktheories.core.DetatchedRandomnessSource;
import org.quicktheories.core.Gen;
import org.quicktheories.core.RandomnessSource;
import org.quicktheories.generators.Generate;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.dates;
import static org.quicktheories.generators.SourceDSL.integers;
import static org.quicktheories.generators.SourceDSL.maps;
import static org.quicktheories.generators.SourceDSL.strings;

public class CustomRandomGen {
  protected static final Logger log = LogManager.getLogger();



  public static void javaBinInput() {
    qt().withGenerateAttempts(10).withShrinkCycles(4)
        .forAll(stdMap(getKey(), getValue(10), 1, 300)).as(map -> new NamedList<Object>((Map<String, ?>) map))
        .check(nl ->  {
          try {
            log.info("namedList: {}\n", Utils.toJSONString(nl.asMap(100)));
            for (Map.Entry<String, Object> obj : nl) {
              if (!(obj.getValue() instanceof Long || obj.getValue() instanceof Map || obj.getValue() instanceof NamedList || obj.getValue() instanceof Integer)) {
                log.error("class was: {}", obj.getValue().getClass().getName());
                throw new Error("class was  " + obj.getValue().getClass().getName());
              }
            }
            return true;
          } catch (Exception e) {
            throw new RuntimeException("Unexpected");
          }
        });
  }


  static Histogram histogram = new Histogram(100, 5);

  static StatisticsCollector collector = new StatisticsCollector("Label");
  public static void javaBinInputSingle() {

     Gen<Map> mapGen = (Gen<Map>) stdMap(getKey(), getValue(10), 1, 300);



    Map map = mapGen.generate(new ExtendedRandomnessSource() {
      @Override
      public long tryNext(Constraint constraints) {
        return next(constraints);
      }

      @Override
      public void add(Precursor other) {

      }

      Well512a rnd = new Well512a();
      RandomDataGenerator rdg = new RandomDataGenerator(rnd);

      @Override
      public long next(Constraint constraints) {
        return rdg.nextZipf((int) (constraints.max() - constraints.min()), 2)  + constraints.min() - 1;
        //return rdg.nextLong(constraints.min(), constraints.max());
      }

      @Override
      public DetatchedRandomnessSource detach() {
        return new ConcreteDetachedSource(this);
      }

      @Override
      public void registerFailedAssumption() {

      }
    });

    log.info("map: {}", map);


//    PrintStream ps = new PrintStream(System.out);
//    histogram.outputPercentileDistribution(ps, 1.0D);
//    ps.close();

    NumberRangeHistogram histogram =new NumberRangeHistogram();

    List<String> report = histogram.formatReport(collector.statisticsEntries());

    System.out.println("report:");

     report.forEach(s ->     System.out.println(s));


    // Map map = mapGen.generate(new

  }

  private static Gen<String> getKey() {
   // return arbitrary().constant("key_").zip(strings().numericBetween(0, 100000), (one, two) -> one + two);
    return strings().betweenCodePoints('a', 'z' + 1).ofLengthBetween(1, 10);
  }

  //stdMap(getKey(), getValue(), 1, 300)).as(map -> new NamedList<Object>(map))

  private static Gen<Object> getValue(int depth) {
    if (depth == 0) {
      return integers().from(1).upToAndIncluding(5000).flatMap(integer -> in -> integer);
    }
    List values = new ArrayList<>();
    values.add(getMapGen(depth - 1));
    values.add(Pair.of(5, new NamedListGen(maps().of(getKey(),  new LazyGen(() -> getValue(depth - 1))).ofSizeBetween(1, 35))));
    values.add(getIntGen());
    return Generate.frequencyWithNoShrinkPoint(values);
  }

  private static Pair<Integer, Gen<?>> getIntGen() {
    return Pair.of(90, tracked(integers().between(150, 300)));
  }

  private static Pair<Integer, Gen<?>> getMapGen(int depth) {
    return Pair.of(5, stdMap(getKey(), new LazyGen(() -> getValue(depth)), 1, 25));
  }


  static Gen<?> tracked(Gen<?> gen) {
    return new TrackingGenerator(gen, collector);
  }

//  private static Pair<Integer, Gen<?>> getNamedListGen(int depth) {
//    return Pair.of(5, rs -> {
//      int size = integers().between(1, 30).generate(rs);
//      Map<String, Object> mapGen = Stream.generate(() -> getKey().generate(rs))
//          .distinct()
//          .map(k -> Collections.singletonMap(k, new LazyGen(() -> getValue(depth)).generate(rs)).entrySet().iterator().next())
//          .limit(size)
//          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
//
//      return new NamedList<>((Map)mapGen);
//    });
//  }

  public static class NamedListGen implements Gen<NamedList> {

    private final Gen<Map<String, Object>> map;

    public NamedListGen(Gen<Map<String,Object>> map) {
      this.map = map;
    }

    @Override
    public NamedList generate(RandomnessSource in) {


      return new NamedList(map.generate(in));
    }
  }


  private static Gen<?> date() {
    return dates().
        withMillisecondsBetween(0, 100000);
//    List values = new ArrayList<>();
//    values.add(getNestedValue());
//    values.add(dates().
//        withMillisecondsBetween(0, 100000));
//
//    return Generate.pick(values);
  }

  private static Gen<Integer> getIntegerGen(int i, int i2) {
    return integers()
        .from(i).upToAndIncluding(i2);
  }


  private static Gen<?> stdMap(Gen<String> keys, Gen<?> values, int lowerSizeBound, int upperSizeBound) {


    return maps().of(keys, values).ofSizeBetween(lowerSizeBound, upperSizeBound);

//    Gen<?> kg;
//    Gen<?> vg;
//    Maps.mapsOf(kg, vg,   Maps.defaultMap() , integers().from(1).upTo(100));

  }

//  private Gen<NamedList> namedLists() {
//    return nesting().zip(childObjectCount(),
//        (nesting, childObjectCount) -> new NamedList(getChildObjects(childObjectCount))).describedAs(r -> "NamedList = " + r)
//        .assuming(namedList -> namedList.size() >= 0);
//  }

  static class LazyGen<T> implements Gen<T> {

    private final Supplier<Gen<T>> genSupplier;
    private Gen<T> gen;

    LazyGen(Supplier<Gen<T>> genSupplier) {
     this.genSupplier = genSupplier;
  }

    @Override
    public T generate(RandomnessSource in) {
      if (this.gen == null) {
        this.gen = genSupplier.get();
      }


      return gen.generate(in);
    }
  }


  public static void main(String[] args) {
    javaBinInputSingle();
  }

  private Gen<Integer> childObjectCount() {
    return integers().from(1).upToAndIncluding(99).describedAs(r -> "childObjectCount = " + r);
  }


  private Map getChildObjects(int childObjects) {
    return new HashMap();
  }

  private Gen<Integer> nesting() {
    return integers().from(0).upToAndIncluding(33).describedAs(r -> "Object Nesting = " + r);
  }


  public static SolrDocument generateSolrDocumentWithChildDocs() {
    SolrDocument parentDocument = new SolrDocument();
    parentDocument.addField("id", "1");
    parentDocument.addField("subject", "parentDocument");

    SolrDocument childDocument = new SolrDocument();
    childDocument.addField("id", "2");
    childDocument.addField("cat", "foo");

    SolrDocument secondKid = new SolrDocument();
    secondKid.addField("id", "22");
    secondKid.addField("cat", "bar");

    SolrDocument grandChildDocument = new SolrDocument();
    grandChildDocument.addField("id", "3");

    childDocument.addChildDocument(grandChildDocument);
    parentDocument.addChildDocument(childDocument);
    parentDocument.addChildDocument(secondKid);

    return parentDocument;
  }




//  private boolean compareObjects(NamedList unmarshaledObj, NamedList matchObj) {
//    log.info("compare objects obj={} obj={}", unmarshaledObj == null ? "(NULL)" : unmarshaledObj.getClass().getName()
//        + ": " + unmarshaledObj, matchObj == null ? "(NULL)" : matchObj.getClass().getName() + ": " + matchObj);
//    //  assertEquals(unmarshaledObj.size(), matchObj.size());
//    for (int i = 0; i < unmarshaledObj.size(); i++) {
//
//      if (unmarshaledObj.getVal(i) instanceof byte[] && matchObj.getVal(i) instanceof byte[]) {
//        byte[] b1 = (byte[]) unmarshaledObj.getVal(i);
//        byte[] b2 = (byte[]) matchObj.getVal(i);
//        return Arrays.equals(b1, b2);
//      } else if (unmarshaledObj.getVal(i) instanceof SolrDocument && matchObj.getVal(i) instanceof SolrDocument) {
//        return(compareSolrDocument(unmarshaledObj.getVal(i), matchObj.getVal(i)));
//        // } else if (unmarshaledObj.get(i) instanceof SolrDocumentList && matchObj.get(i) instanceof SolrDocumentList) {
//        //    return(unmarshaledObj.get(i) + " vs " + matchObj.get(i), SolrTestUtil.compareSolrDocumentList(unmarshaledObj.get(i), matchObj.get(i)));
//        //   } else if (unmarshaledObj.get(i) instanceof SolrInputDocument && matchObj.get(i) instanceof SolrInputDocument) {
//        //    return("unmarshalled:\n" + unmarshaledObj.get(i) + "]\nobj:\n" + matchObj.get(i) , compareSolrInputDocument(unmarshaledObj.get(i), matchObj.get(i)));
//        //   } else if (unmarshaledObj.get(i) instanceof SolrInputField && matchObj.get(i) instanceof SolrInputField) {
//        //       return(assertSolrInputFieldEquals(unmarshaledObj.get(i), matchObj.get(i)));
//      } else {
//        Object obj = unmarshaledObj.getVal(i);
//        log.info("unmarshaledObj={} matchObj={}", obj, matchObj.getVal(i));
//        return matchObj.getVal(i).equals(obj);
//      }
//
//    }
//    return false;
//  }



//
//  private static byte[] getBytes(Object o) throws Exception {
//    ByteArrayOutputStream baos = new ByteArrayOutputStream();
//    try (JavaBinCodec javabin = new JavaBinCodec(baos)) {
//      javabin.marshal(o);
//      return baos.toByteArray();
//    }
//  }

//  private static byte[] getBytes(Object o, boolean readAsCharSeq) throws Exception {
//    ByteArrayOutputStream baos = new ByteArrayOutputStream();
//    try (JavaBinCodec javabin = new JavaBinCodec(baos)) {
//      javabin.setReadStringAsCharSeq(readAsCharSeq);
//      javabin.marshal(o);
//      return baos.toByteArray();
//    }
//  }

  private static Object getObject(byte[] bytes) throws IOException, InterruptedException {
    try (JavaBinCodec jbc = new JavaBinCodec()) {
      return jbc.unmarshal(new ByteArrayInputStream(bytes));
    }
  }

}

