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

import org.quicktheories.generators.ArbitraryDSL;
import org.quicktheories.generators.ArraysDSL;
import org.quicktheories.generators.BigDecimalsDSL;
import org.quicktheories.generators.BigIntegersDSL;
import org.quicktheories.generators.BooleansDSL;
import org.quicktheories.generators.CharactersDSL;
import org.quicktheories.generators.DatesDSL;
import org.quicktheories.generators.DoublesDSL;
import org.quicktheories.generators.FloatsDSL;
import org.quicktheories.generators.ListsDSL;
import org.quicktheories.generators.LocalDatesDSL;
import org.quicktheories.generators.LongsDSL;
import org.quicktheories.generators.MapsDSL;

public class SourceDSL {

  public static LongsDSL longs() {
    return new LongsDSL();
  }

  public static IntegersDSL integers() {
    return new IntegersDSL();
  }

  public static DoublesDSL doubles() {
    return new DoublesDSL();
  }

  public static FloatsDSL floats() {
    return new FloatsDSL();
  }

  public static CharactersDSL characters() {
    return new CharactersDSL();
  }

  public static StringsDSL strings() {
    return new StringsDSL();
  }

  public static ListsDSL lists() {
    return new ListsDSL();
  }

  public static MapsDSL maps() {
    return new MapsDSL();
  }

  public static ArraysDSL arrays() {
    return new ArraysDSL();
  }

  public static BigIntegersDSL bigIntegers() {
    return new BigIntegersDSL();
  }

  public static BigDecimalsDSL bigDecimals() {
    return new BigDecimalsDSL();
  }

  public static ArbitraryDSL arbitrary() {
    return new ArbitraryDSL();
  }

  public static DatesDSL dates() {
    return new DatesDSL();
  }

  public static LocalDatesDSL localDates() {
    return new LocalDatesDSL();
  }

  public static BooleansDSL booleans() {
    return new BooleansDSL();
  }

  public static void checkArguments(
      boolean expression, String errorMessageTemplate, Object... errorMessageArgs) {
    if (!expression) {
      throw new IllegalArgumentException(String.format(errorMessageTemplate, errorMessageArgs));
    }
  }
}
