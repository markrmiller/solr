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

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.common.SolrInputDocument;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Random;
import java.util.SplittableRandom;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class DocMakerRamGen {

  private final static Map<String,Queue<SolrInputDocument>> CACHE = new ConcurrentHashMap<>();

  private Queue<SolrInputDocument> docs = new ConcurrentLinkedQueue<>();

  private final Map<String, FieldDef> fields = new HashMap<>();

  private static final AtomicInteger ID = new AtomicInteger();
  private final boolean cacheResults;

  private ExecutorService executorService;

  private SplittableRandom threadRandom;

  public DocMakerRamGen() {
   this(true);
 }

  public DocMakerRamGen(boolean cacheResults) {
    this.cacheResults = cacheResults;

    Long seed = Long.getLong("threadLocalRandomSeed");
    if (seed == null) {
      System.setProperty("threadLocalRandomSeed", Long.toString(new Random().nextLong()));
    }

    threadRandom = new SplittableRandom(Long.getLong("threadLocalRandomSeed"));
  }

  public void preGenerateDocs(int numDocs) throws InterruptedException {
    executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() + 1);
    if (cacheResults) {
      docs = CACHE.compute(Integer.toString(hashCode()), (key, value) -> {
        if (value == null) {
          for (int i = 0; i < numDocs; i++) {
            executorService.submit(() -> {
              SolrInputDocument doc = getDocument();
              docs.add(doc);
            });
          }
          return docs;
        }
        for (int i = value.size(); i < numDocs; i++) {
          executorService.submit(() -> {
            SolrInputDocument doc = getDocument();
            value.add(doc);
          });
        }
        return value;
      });
    } else {
      for (int i = 0; i < numDocs; i++) {
        executorService.submit(() -> {
          SolrInputDocument doc = getDocument();
          docs.add(doc);
        });
      }
    }

    executorService.shutdown();
    boolean result = executorService.awaitTermination(10, TimeUnit.MINUTES);
    if (!result) {
      throw new RuntimeException("Timeout waiting for doc adds to finish");
    }
  }

  public Iterator<SolrInputDocument> getGeneratedDocsIterator() {
    return docs.iterator();
  }

  public SolrInputDocument getDocument() {
    SolrInputDocument doc = new SolrInputDocument();

    for (Map.Entry<String,FieldDef> entry : fields.entrySet()) {
      doc.addField(entry.getKey(), getValue(entry.getValue()));
    }

    return doc;
  }

  public void addField(String name, FieldDef.FieldDefBuilder builder) {
    fields.put(name, builder.build());
  }

  private Object getValue(FieldDef value) {
    switch (value.getContent()) {
      case UNIQUE_INT:
        return ID.incrementAndGet();
      case INTEGER:
        if (value.getMaxCardinality() > 0) {
          long start = value.getCardinalityStart();
          long seed = nextLong(start, start + value.getMaxCardinality(), threadRandom);
          SplittableRandom random = new SplittableRandom(seed);
          return nextInt(0, Integer.MAX_VALUE, random);
        }

        return ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE);
      case ALPHEBETIC:
        if (value.getNumTokens() > 1) {
          StringBuilder sb = new StringBuilder(value.getNumTokens() * (Math.max(value.getLength(),value.getMaxLength()) + 1));
          for (int i = 0; i < value.getNumTokens(); i++) {
            if (i > 0) {
              sb.append(' ');
            }
            sb.append(getAlphabeticString(value));
          }
          return sb.toString();
        }
        return getAlphabeticString(value);
      case UNICODE:
        if (value.getNumTokens() > 1) {
          StringBuilder sb = new StringBuilder(value.getNumTokens() * (Math.max(value.getLength(),value.getMaxLength()) + 1));
          for (int i = 0; i < value.getNumTokens(); i++) {
            if (i > 0) {
              sb.append(' ');
            }
            sb.append(getUnicodeString(value));
          }
          return sb.toString();
        }
        return getUnicodeString(value);
      default:
        throw new UnsupportedOperationException("Unsupported content type type=" + value.getContent());
    }

  }

  private String getUnicodeString(FieldDef value) {
    if (value.getMaxCardinality() > 0) {
      long start = value.getCardinalityStart();
      long seed = nextLong(start, start + value.getMaxCardinality(), threadRandom);
      SplittableRandom random = new SplittableRandom(seed);
      if (value.getLength() > -1) {
        return TestUtil.randomRealisticUnicodeString(new Random(seed), value.getLength(), value.getLength());
      } else {
        return TestUtil.randomRealisticUnicodeString(new Random(seed), 1, value.getMaxLength());
      }
    }

    if (value.getLength() > -1) {
      return TestUtil.randomRealisticUnicodeString(ThreadLocalRandom.current(), value.getLength(), value.getLength());
    } else {
      return TestUtil.randomRealisticUnicodeString(ThreadLocalRandom.current(), 1, value.getMaxLength());
    }
  }

  private String getAlphabeticString(FieldDef value) {
    if (value.getMaxCardinality() > 0) {
      long start = value.getCardinalityStart();
      long seed = nextLong(start, start + value.getMaxCardinality(), threadRandom);
      SplittableRandom random = new SplittableRandom(seed);
      if (value.getLength() > -1) {
        return RandomStringUtils.random(nextInt(value.getLength(), value.getLength(), random), 0, 0, true, false, null, new Random(seed));
      } else {
        return RandomStringUtils.random(nextInt(1, value.getMaxLength(), random), 0, 0, true, false, null, new Random(seed));
      }
    }

    SplittableRandom threadRandom = new SplittableRandom(Long.getLong("threadLocalRandomSeed", ThreadLocalRandom.current().nextLong()));
    if (value.getLength() > -1) {
      return RandomStringUtils.random(nextInt(value.getLength(), value.getLength(), threadRandom), 0, 0, true, false, null, ThreadLocalRandom.current());
    } else {
      return RandomStringUtils.random(nextInt(1, value.getMaxLength(), threadRandom), 0, 0, true, false, null, ThreadLocalRandom.current());
    }
  }

  public enum Content {
    UNICODE, ALPHEBETIC, INTEGER, UNIQUE_INT
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DocMakerRamGen that = (DocMakerRamGen) o;
    return fields.equals(that.fields);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fields);
  }

  public static int nextInt(final int startInclusive, final int endExclusive, SplittableRandom random) {
    Validate.isTrue(endExclusive >= startInclusive,
        "Start value must be smaller or equal to end value.");
    Validate.isTrue(startInclusive >= 0, "Both range values must be non-negative.");

    if (startInclusive == endExclusive) {
      return startInclusive;
    }

    return startInclusive + random.nextInt(endExclusive - startInclusive);
  }

  public static long nextLong(final long startInclusive, final long endExclusive, SplittableRandom random) {
    Validate.isTrue(endExclusive >= startInclusive,
        "Start value must be smaller or equal to end value.");
    Validate.isTrue(startInclusive >= 0, "Both range values must be non-negative.");

    if (startInclusive == endExclusive) {
      return startInclusive;
    }

    return startInclusive + random.nextLong(endExclusive - startInclusive);
  }
}
