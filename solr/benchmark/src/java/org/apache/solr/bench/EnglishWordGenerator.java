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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.SplittableRandom;
import org.eclipse.jetty.io.RuntimeIOException;
import org.quicktheories.core.Gen;
import org.quicktheories.core.RandomnessSource;

public class EnglishWordGenerator implements Gen<String> {

  private final CounterGenerator counterGenerator;
  private final ZipfianGenerator zipfianGenerator;

  private final List<String> words;

  private int size;
  private String lastValue;

  public EnglishWordGenerator(SplittableRandom random) {

    try {
      words = Files.readAllLines(Paths.get("src", "resources", "words.txt"));
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
    this.size = words.size();
    this.counterGenerator = new CounterGenerator(0);
    this.zipfianGenerator = new ZipfianGenerator(size, random);
  }

  @Override
  public String generate(RandomnessSource in) {
    String value = words.get(zipfianGenerator.nextLong(size, in).intValue());
    lastValue = value;
    return value;
  }
}
