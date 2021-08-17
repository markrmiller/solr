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

import java.util.SplittableRandom;
import org.quicktheories.core.Gen;
import org.quicktheories.generators.Generate;
import org.quicktheories.impl.BenchmarkRandomSource;

public class StringsDSL {

  private static final int BASIC_LATIN_LAST_CODEPOINT = 0x007E;
  private static final int BASIC_LATIN_FIRST_CODEPOINT = 0x0020;
  private static final int ASCII_LAST_CODEPOINT = 0x007F;
  private static final int LARGEST_DEFINED_BMP_CODEPOINT = 65533;

  public SolrGen<String> multi(int startInclusive, int endInclusive, Gen<String> strings) {
    return  new SolrGen<>(
        Generate.range(startInclusive, endInclusive)
            .mutate(
                (base, randomness) -> {
                  StringBuilder sb = new StringBuilder();
                  for (int i = 0; i < base; i++) {
                    sb.append(strings.generate(randomness));
                    if (i < base - 1) {
                      sb.append(' ');
                    }
                  }
                  return sb.toString();
                }));
  }

  public SolrGen<String> multi(int endInclusive, Gen<String> strings) {
    return multi(1, endInclusive, strings);
  }

  /**
   * Generates integers as Strings, and shrinks towards "0".
   *
   * @return a Source of type String
   */
  public SolrGen<String> numeric() {
    return new SolrGen<>(numericBetween(Integer.MIN_VALUE, Integer.MAX_VALUE));
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
    return new SolrGen<>(Strings.boundedNumericStrings(startInclusive, endInclusive));
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

  public static class StringGeneratorBuilder {

    private final int minCodePoint;
    private final int maxCodePoint;

    private Integer cardinalityStart;
    private int maxCardinality;

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
      return new SolrGen<>(Strings.withCodePoints(minCodePoint, maxCodePoint, Generate.constant(codePoints)));
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
      Gen<String> strings = Strings.ofBoundedLengthStrings(minCodePoint, maxCodePoint, minLength, maxLength);

      if (maxCardinality > 0) {
        return new SolrGen<>(in -> {
          if (cardinalityStart == null) {
            cardinalityStart = Generate.range(0, Integer.MAX_VALUE - maxCardinality).generate(in);
          }

          long seed = Generate.range(cardinalityStart, cardinalityStart + maxCardinality).generate(in);
          return strings.generate(new BenchmarkRandomSource(new SplittableRandom(seed)));
        });
      } else {
        return new SolrGen<>(strings);
      }
    }
  }
}
