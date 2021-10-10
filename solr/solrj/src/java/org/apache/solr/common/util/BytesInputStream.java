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

import java.io.IOException;
import java.io.InputStream;

/** An un-synchronized byte[] InputStream. */
public final class BytesInputStream extends InputStream {

  private byte[] buf;
  private int pos;
  private int mark;
  private final int count;


  public BytesInputStream(byte[] buf) {
    mark = 0;
    this.buf = buf;
    count = buf.length;
  }

  public BytesInputStream(byte[] buf, int offset, int length) {
    this.buf = buf;
    pos = offset;
    mark = offset;
    count = offset + length > buf.length ? buf.length : offset + length;
  }


  @Override
  public int available() {
    return count - pos;
  }


  @Override
  public void close() throws IOException {
    // Do nothing on close, this matches JDK behaviour.
  }


  @Override
  public void mark(int readlimit) {
    mark = pos;
  }

  @Override
  public boolean markSupported() {
    return true;
  }

  @Override
  public int read() {
    return pos < count ? buf[pos++] & 0xFF : -1;
  }

  @Override
  public int read(byte[] b, int offset, int length) {
    if (b == null) {
      throw new NullPointerException();
    }
    // avoid int overflow
    if (offset < 0 || offset > b.length || length < 0 || length > b.length - offset) {
      throw new IndexOutOfBoundsException();
    }
    // Are there any bytes available?
    if (pos >= count) {
      return -1;
    }
    if (length == 0) {
      return 0;
    }

    int copylen = count - pos < length ? count - pos : length;
    System.arraycopy(buf, pos, b, offset, copylen);
    pos += copylen;
    return copylen;
  }

  @Override
  public void reset() {
    pos = mark;
  }

  @Override
  public long skip(long n) {
    if (n <= 0) {
      return 0;
    }
    int temp = pos;
    pos = count - pos < n ? count : (int) (pos + n);
    return pos - (long)temp;
  }
}
