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

import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

public class FieldDef {
  private DocMakerRamGen.Content content;
  private int numTokens = 1;
  private int maxCardinality = -1;
  private int maxLength = 64;
  private int length = -1;
  private long cardinalityStart;

  public int getNumTokens() {
    return numTokens;
  }

  public int getMaxCardinality() {
    return maxCardinality;
  }

  public long getCardinalityStart() {
    return cardinalityStart;
  }

  public int getMaxLength() {
    return maxLength;
  }

  public int getLength() {
    return length;
  }

  public DocMakerRamGen.Content getContent() {
    return content;
  }

  public static final class FieldDefBuilder {

    private DocMakerRamGen.Content content;
    private int numTokens = 1;
    private int maxCardinality = -1;
    private int maxLength = 64;
    private int length = -1;
    private long cardinalityStart;

    private FieldDefBuilder() {
    }

    public static FieldDefBuilder aFieldDef() {
      return new FieldDefBuilder();
    }

    public FieldDefBuilder withContent(DocMakerRamGen.Content content) {
      this.content = content;
      return this;
    }

    public FieldDefBuilder withTokenCount(int numTokens) {
      if (numTokens > 1 && content == DocMakerRamGen.Content.UNIQUE_INT) {
        throw new UnsupportedOperationException("UNIQUE_INT content type cannot be used with token count > 1");
      }
      this.numTokens = numTokens;
      return this;
    }

    public FieldDefBuilder withMaxCardinality(int maxCardinality) {
      if (numTokens > 1) {
        throw new UnsupportedOperationException("maxCardinality cannot be used with token count > 1");
      }
      this.maxCardinality = maxCardinality;
      this.cardinalityStart = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE - maxCardinality);
      return this;
    }

    public FieldDefBuilder withMaxLength(int maxLength) {
      if (length > -1) {
        throw new UnsupportedOperationException("maxLength cannot be used with maxLength");
      }
      this.maxLength = maxLength;
      return this;
    }

    public FieldDefBuilder withLength(int length) {
      this.length = length;
      return this;
    }

    public FieldDef build() {
      FieldDef fieldDef = new FieldDef();
      fieldDef.numTokens = this.numTokens;
      fieldDef.content = this.content;
      fieldDef.maxCardinality = this.maxCardinality;
      fieldDef.maxLength = this.maxLength;
      fieldDef.cardinalityStart = this.cardinalityStart;
      return fieldDef;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    FieldDef fieldDef = (FieldDef) o;
    return numTokens == fieldDef.numTokens && maxCardinality == fieldDef.maxCardinality && maxLength == fieldDef.maxLength && length == fieldDef.length && content == fieldDef.content;
  }

  @Override
  public int hashCode() {
    return Objects.hash(content, numTokens, maxCardinality, maxLength, length);
  }
}
