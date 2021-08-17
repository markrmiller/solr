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

import java.util.List;
import java.util.SplittableRandom;
import org.quicktheories.core.Gen;
import org.quicktheories.core.RandomnessSource;
import org.quicktheories.generators.Generate;
import org.quicktheories.impl.BenchmarkRandomSource;

public class StringsDSL {

  private static final boolean  TRACK = false;

  private static final int BASIC_LATIN_LAST_CODEPOINT = 0x007E;
  private static final int BASIC_LATIN_FIRST_CODEPOINT = 0x0020;
  private static final int ASCII_LAST_CODEPOINT = 0x007F;
  private static final int LARGEST_DEFINED_BMP_CODEPOINT = 65533;

  static StatisticsCollector collector = new StatisticsCollector("Label");

  private Gen<String> maybeTrack(Gen<String> gen) {
    if (!TRACK) return gen;
    return new TrackingGenerator<>(gen, collector);
  }

  public static void printReport() {
    // NumberRangeHistogram histogram =new NumberRangeHistogram();
    Histogram histogram = new Histogram();
    List<String> report = histogram.formatReport(collector.statisticsEntries());

    System.out.println("report:");

    report.forEach(s -> System.out.println(s));
  }

  public Gen<String> multiString(int startInclusive, int endInclusive, Gen<String> strings) {
    return maybeTrack(
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

  public Gen<String> multiString(int endInclusive, Gen<String> strings) {
    return multiString(1, endInclusive, strings);
  }

  public Gen<String> maxCardinality(int max, Gen<String> strings, RandomnessSource random) {
    return maybeTrack(
        new Gen<>() {
          int cardinalityStart = Generate.range(0, Integer.MAX_VALUE - max).generate(random);

          @Override
          public String generate(RandomnessSource in) {
            long seed = Generate.range(cardinalityStart, cardinalityStart + max).generate(in);
            return strings.generate(new BenchmarkRandomSource(new SplittableRandom(seed)));
          }
        });
  }

  /**
   * Generates integers as Strings, and shrinks towards "0".
   *
   * @return a Source of type String
   */
  public Gen<String> numeric() {
    return maybeTrack(numericBetween(Integer.MIN_VALUE, Integer.MAX_VALUE));
  }

  /**
   * Generates integers within the interval as Strings.
   *
   * @param startInclusive - lower inclusive bound of integer domain
   * @param endInclusive - upper inclusive bound of integer domain
   * @return a Source of type String
   */
  public Gen<String> numericBetween(int startInclusive, int endInclusive) {
    checkArguments(
        startInclusive <= endInclusive,
        "There are no Integer values to be generated between startInclusive (%s) and endInclusive (%s)",
        startInclusive,
        endInclusive);
    return maybeTrack(Strings.boundedNumericStrings(startInclusive, endInclusive));
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
    public Gen<String> ofFixedNumberOfCodePoints(int codePoints) {
      checkArguments(
          codePoints >= 0,
          "The number of codepoints cannot be negative; %s is not an accepted argument",
          codePoints);
      return Strings.withCodePoints(minCodePoint, maxCodePoint, Generate.constant(codePoints));
    }

    /**
     * Generates Strings of a fixed length.
     *
     * @param fixedLength - the fixed length for the Strings
     * @return a Source of type String
     */
    public Gen<String> ofLength(int fixedLength) {
      return ofLengthBetween(fixedLength, fixedLength);
    }

    /**
     * Generates Strings of length bounded between minLength and maxLength inclusively.
     *
     * @param minLength - minimum inclusive length of String
     * @param maxLength - maximum inclusive length of String
     * @return a Source of type String
     */
    public Gen<String> ofLengthBetween(int minLength, int maxLength) {
      checkArguments(
          minLength <= maxLength,
          "The minLength (%s) is longer than the maxLength(%s)",
          minLength,
          maxLength);
      checkArguments(
          minLength >= 0,
          "The length of a String cannot be negative; %s is not an accepted argument",
          minLength);
      return Strings.ofBoundedLengthStrings(minCodePoint, maxCodePoint, minLength, maxLength);
    }
  }
}
