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

import java.util.function.Function;
import org.quicktheories.core.Gen;
import org.quicktheories.generators.CodePoints;
import org.quicktheories.generators.Generate;

final class Strings {

  static SolrGen<String> boundedNumericStrings(int startInclusive, int endInclusive) {
    return new SolrGen<>(Generate.range(startInclusive, endInclusive).map(i -> i.toString()));
  }

  static SolrGen<String> withCodePoints(
      int minCodePoint, int maxCodePoint, Gen<Integer> numberOfCodePoints) {

    return new SolrGen<>(Generate.intArrays(numberOfCodePoints, CodePoints.codePoints(minCodePoint, maxCodePoint))
        .map(is -> new String(is, 0, is.length)));
  }

  static SolrGen<String> ofBoundedLengthStrings(
      int minCodePoint, int maxCodePoint, int minLength, int maxLength) {

    // generate strings of fixed number of code points then modify any that exceed max length
    return new SolrGen<>(withCodePoints(minCodePoint, maxCodePoint, Generate.range(minLength, maxLength))
        .map(reduceToSize(maxLength)));
  }

  private static Function<String, String> reduceToSize(int maxLength) {
    // Reduce size of string by removing characters from start
    return s -> {
      if (s.length() <= maxLength) {
        return s;
      }
      String t = s;
      while (t.length() > maxLength) {
        t = t.substring(1);
      }
      return t;
    };
  }
}
