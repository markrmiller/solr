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
package org.apache.solr.common.util;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

/** Single threaded buffered InputStream
 *  Internal Solr use only, subject to change.
 */
public class SolrInputStream extends DataInputStream implements DataInputInputStream, DataInput {

  public SolrInputStream(InputStream in) {
    super(in);
  }

  @Override
  public boolean readDirectUtf8(ByteArrayUtf8CharSequence utf8, int len) {
    return false;
  }

  public static SolrInputStream wrap(InputStream in) {
    return (in instanceof SolrInputStream) ? (SolrInputStream)in : new SolrInputStream(in);
  }

}
