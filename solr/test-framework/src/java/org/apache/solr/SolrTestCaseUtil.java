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
package org.apache.solr;

import org.apache.lucene.util.LuceneTestCase;

public class SolrTestCaseUtil {
  /**
   * Checks a specific exception class is thrown by the given runnable, and returns it.
   */
  public static <T extends Throwable> T expectThrows(Class<T> expectedType, LuceneTestCase.ThrowingRunnable runnable) {
    return expectThrows(expectedType, "Expected exception " + expectedType.getSimpleName() + " but no exception was thrown", runnable);
  }

  /**
   * Checks a specific exception class is thrown by the given runnable, and returns it.
   */
  public static <T extends Throwable> T expectThrows(Class<T> expectedType, String noExceptionMessage, LuceneTestCase.ThrowingRunnable runnable) {
    return LuceneTestCase.expectThrows(expectedType, noExceptionMessage, runnable);
  }

  public static <T> T pickRandom(T... options) {
    return options[SolrTestCase.random().nextInt(options.length)];
  }
}