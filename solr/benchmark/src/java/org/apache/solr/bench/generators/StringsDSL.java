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
package org.apache.solr.bench.generators;

import static org.apache.solr.bench.generators.SourceDSL.checkArguments;
import static org.apache.solr.bench.generators.SourceDSL.integers;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.SplittableRandom;

import org.eclipse.jetty.io.RuntimeIOException;
import org.quicktheories.core.RandomnessSource;
import org.quicktheories.generators.Generate;
import org.quicktheories.impl.BenchmarkRandomSource;
import org.quicktheories.impl.SplittableRandomSource;

public class StringsDSL {

  private static final int BASIC_LATIN_LAST_CODEPOINT = 0x007E;
  private static final int BASIC_LATIN_FIRST_CODEPOINT = 0x0020;
  private static final int ASCII_LAST_CODEPOINT = 0x007F;
  private static final int LARGEST_DEFINED_BMP_CODEPOINT = 65533;

  private static final List<String> words;

  private static final int wordsSize;

  static {
    try {
      words = Files.readAllLines(Paths.get("src", "resources", "words.txt"));
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
    wordsSize = words.size();
  }

  public WordListGeneratorBuilder wordList() {
    return new WordListGeneratorBuilder(new SolrGen(new SolrGen<String>(Type.String) {
      @Override
      public String generate(RandomnessSource in) {
        return words.get(integers().between(0, wordsSize - 1).withDistribution(this.getDistribution()).generate(in));
      }
    }, Type.String));
  }


  /**
   * Generates integers as Strings, and shrinks towards "0".
   *
   * @return a Source of type String
   */
  public SolrGen<String> numeric() {
    return new SolrGen<>(numericBetween(Integer.MIN_VALUE, Integer.MAX_VALUE), Type.String);
  }

  /**
   * Generates integers within the interval as Strings.
   *
   * @param startInclusive - lower inclusive bound of integer domain
   * @param endInclusive - upper inclusive bound of integer domain
   * @return a Source of type String
   */
  public SolrGen<String> numericBetween(int startInclusive, int endInclusive) {
    checkArguments(
        startInclusive <= endInclusive,
        "There are no Integer values to be generated between startInclusive (%s) and endInclusive (%s)",
        startInclusive,
        endInclusive);
    return new SolrGen<>(Strings.boundedNumericStrings(startInclusive, endInclusive), Type.String);
  }

  /**
   * Constructs a StringGeneratorBuilder which will build Strings composed from all defined code
   * points
   *
   * @return a StringGeneratorBuilder
   */
  public StringGeneratorBuilder allPossible() {
    return betweenCodePoints(Character.MIN_CODE_POINT, Character.MAX_CODE_POINT);
  }

  /**
   * Constructs a StringGeneratorBuilder which will build Strings composed from all defined code
   * points in the Basic Multilingual Plane
   *
   * @return a StringGeneratorBuilder
   */
  public StringGeneratorBuilder basicMultilingualPlaneAlphabet() {
    return betweenCodePoints(Character.MIN_CODE_POINT, LARGEST_DEFINED_BMP_CODEPOINT);
  }

  /**
   * Constructs a StringGeneratorBuilder which will build Strings composed from Unicode Basic Latin
   * Alphabet
   *
   * @return a StringGeneratorBuilder
   */
  public StringGeneratorBuilder basicLatinAlphabet() {
    return betweenCodePoints(BASIC_LATIN_FIRST_CODEPOINT, BASIC_LATIN_LAST_CODEPOINT);
  }

  public StringGeneratorBuilder alpha() {
    return betweenCodePoints('a', 'z' + 1);
  }

  public StringGeneratorBuilder alphaNumeric() {
    return betweenCodePoints(' ', 'z' + 1);
  }

  /**
   * Constructs a StringGeneratorBuilder which will build Strings composed from Unicode Ascii
   * Alphabet
   *
   * @return a StringGeneratorBuilder
   */
  public StringGeneratorBuilder ascii() {
    return betweenCodePoints(Character.MIN_CODE_POINT, ASCII_LAST_CODEPOINT);
  }

  /**
   * Strings with characters between two (inclusive) code points
   *
   * @param minInclusive minimum code point
   * @param maxInclusive max code point
   * @return Builder for strings
   */
  public StringGeneratorBuilder betweenCodePoints(int minInclusive, int maxInclusive) {
    return new StringGeneratorBuilder(minInclusive, maxInclusive);
  }

  public static class WordListGeneratorBuilder {
    private SolrGen<String> strings;

    WordListGeneratorBuilder(SolrGen<String> strings) {
      this.strings = strings;
    }

    public SolrGen<String> ofOne() {
      return strings;
    }

    public SolrGen<String> multi(int count) {
      return multiStringGen(strings, count);
    }

    public WordListGeneratorBuilder tracked(StatCollector collector) {
      this.strings = this.strings.tracked(collector);
      return this;
    }

    public WordListGeneratorBuilder withDistribution(Distribution distribution) {
      this.strings.withDistribution(distribution);
      return this;
    }
  }

  public static class StringGeneratorBuilder {

    private final int minCodePoint;
    private final int maxCodePoint;
    private Integer cardinalityStart;
    private int maxCardinality;
    private int multi;

    private StringGeneratorBuilder(int minCodePoint, int maxCodePoint) {
      this.minCodePoint = minCodePoint;
      this.maxCodePoint = maxCodePoint;
    }

    /**
     * Generates Strings of a fixed number of code points.
     *
     * @param codePoints - the fixed number of code points for the String
     * @return a a Source of type String
     */
    public SolrGen<String> ofFixedNumberOfCodePoints(int codePoints) {
      checkArguments(
          codePoints >= 0,
          "The number of codepoints cannot be negative; %s is not an accepted argument",
          codePoints);
      return new SolrGen<>(Strings.withCodePoints(minCodePoint, maxCodePoint, Generate.constant(codePoints)), Type.String);
    }

    /**
     * Generates Strings of a fixed length.
     *
     * @param fixedLength - the fixed length for the Strings
     * @return a Source of type String
     */
    public SolrGen<String> ofLength(int fixedLength) {
      return ofLengthBetween(fixedLength, fixedLength);
    }

    public StringGeneratorBuilder maxCardinality(int max) {
      maxCardinality = max;
      return this;
    }

    public StringGeneratorBuilder multi(int count) {
      this.multi = count;
      return this;
    }

//    StringBuilder sb = new StringBuilder();
//                    for (int i = 0; i < base; i++) {
//      sb.append(strings.generate(randomness));
//      if (i < base - 1) {
//        sb.append(' ');
//      }
//    }
//                    return sb.toString();

    /**
     * Generates Strings of length bounded between minLength and maxLength inclusively.
     *
     * @param minLength - minimum inclusive length of String
     * @param maxLength - maximum inclusive length of String
     * @return a Source of type String
     */
    public SolrGen<String> ofLengthBetween(int minLength, int maxLength) {
      checkArguments(
          minLength <= maxLength,
          "The minLength (%s) is longer than the maxLength(%s)",
          minLength,
          maxLength);
      checkArguments(
          minLength >= 0,
          "The length of a String cannot be negative; %s is not an accepted argument",
          minLength);
      SolrGen<String> strings = Strings.ofBoundedLengthStrings(minCodePoint, maxCodePoint, minLength, maxLength);

      if (maxCardinality > 0) {
        SolrGen<String> gen = new SolrGen<>(in -> {
          if (cardinalityStart == null) {
            cardinalityStart = Generate.range(0, Integer.MAX_VALUE - maxCardinality - 1).generate(in);
          }

          long seed = Generate.range(cardinalityStart, cardinalityStart + maxCardinality - 1).generate(in);
          return strings.generate(new SplittableRandomSource(new SplittableRandom(seed)));
        }, Type.String);
        if (multi > 1) {
          return multiStringGen(gen, multi);
        }
        return new SolrGen<>(gen, Type.String);
      } else {
        if (multi > 1) {
          return multiStringGen(strings, multi);
        }
        return new SolrGen<>(strings, Type.String);
      }
    }


  }

  private static SolrGen<String> multiStringGen(SolrGen<String> strings, int multi) {
    return new SolrGen<>(Type.MultiString) {
      @Override
      public String generate(RandomnessSource in) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < multi; i++) {
          sb.append(strings.generate(((BenchmarkRandomSource)in)));
          if (i < multi - 1) {
            sb.append(' ');
          }
        }
        return sb.toString();
      }
    };
  }
}
