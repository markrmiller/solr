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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;


/**
 * A subclass of {@link FastOutputStream} that avoids all buffering and writes directly to the {@link OutputStream}.
 */
public final class AlreadyBufferedOutputStream extends FastOutputStream {
  protected final OutputStream out;

  protected long written;  // how many bytes written to the underlying stream
  protected int pos;


  public AlreadyBufferedOutputStream(OutputStream w) {
    super(null);
    if (w instanceof FastOutputStream) {
      throw new IllegalArgumentException();
    }
    this.out = w;
  }


  public static AlreadyBufferedOutputStream wrap(OutputStream sink) {
    if (sink instanceof FastOutputStream && !(sink instanceof AlreadyBufferedOutputStream)) {
      throw new IllegalArgumentException("Cannot pass a FastOutputStream here");
    }
    return (sink instanceof AlreadyBufferedOutputStream) ? (AlreadyBufferedOutputStream) sink : new AlreadyBufferedOutputStream(sink);
  }

  @Override
  public void write(int b) throws IOException {
    out.write((byte) b);
  }

  @Override
  public void write(byte b[]) throws IOException {
    out.write(b, 0, b.length);
  }

  public void write(byte b) throws IOException {
    out.write(b);
  }

  @Override
  public void write(byte arr[], int off, int len) throws IOException {
    out.write(arr, off, len);
  }

  ////////////////// DataOutput methods ///////////////////
  @Override
  public void writeBoolean(boolean v) throws IOException {
    if (v) {
      out.write((byte) 1);
    } else {
      out.write((byte) 0);
    }
  }

  @Override
  public void writeByte(int v) throws IOException {
    out.write((byte) v);
  }

  @Override
  public void writeShort(int v) throws IOException {
    write((byte) (v >>> 8));
    write((byte) v);
  }

  @Override
  public void writeChar(int v) throws IOException {
    writeShort(v);
  }

  @Override
  public void writeInt(int v) throws IOException {
    out.write((byte) (v >>> 24));
    out.write((byte) (v >>> 16));
    out.write((byte) (v >>> 8));
    out.write((byte) (v));
    pos += 4;
  }

  @Override
  public void writeLong(long v) throws IOException {
    out.write((byte) (v >>> 56));
    out.write((byte) (v >>> 48));
    out.write((byte) (v >>> 40));
    out.write((byte) (v >>> 32));
    out.write((byte) (v >>> 24));
    out.write((byte) (v >>> 16));
    out.write((byte) (v >>> 8));
    out.write((byte) (v));
    pos += 8;
  }

  @Override
  public void writeFloat(float v) throws IOException {
    writeInt(Float.floatToRawIntBits(v));
  }

  @Override
  public void writeDouble(double v) throws IOException {
    writeLong(Double.doubleToRawLongBits(v));
  }

  @Override
  public void writeBytes(String s) throws IOException {
    // non-optimized version, but this shouldn't be used anyway
    for (int i = 0; i < s.length(); i++)
      write((byte) s.charAt(i));
  }

  @Override
  public void writeChars(String s) throws IOException {
    // non-optimized version
    for (int i = 0; i < s.length(); i++)
      writeChar(s.charAt(i));
  }

  @Override
  public void writeUTF(String s) throws IOException {
    // non-optimized version, but this shouldn't be used anyway
    DataOutputStream daos = new DataOutputStream(this);
    daos.writeUTF(s);
  }


  @Override
  public void flush() throws IOException {
    if (out != null) out.flush();
  }

  @Override
  public void close() throws IOException {
    if (out != null) out.close();
  }

  /**
   * Only flushes the buffer of the FastOutputStream, not that of the underlying stream.
   */
  public void flushBuffer() throws IOException {

  }

  /** All writes to the sink will go through this method */
  public void flush(byte[] buf, int offset, int len) throws IOException {
    out.write(buf, offset, len);
  }

  public long size() {
    return written + pos;
  }

  public void reserve(int len) throws IOException {

  }

  /**
   * Returns the number of bytes actually written to the underlying OutputStream, not including anything currently
   * buffered by this class itself.
   */
  public long written() {
    return written;
  }

  /** Resets the count returned by written() */
  public void setWritten(long written) {
    this.written = written;
  }

  /**
   * Copies a {@link Utf8CharSequence} without making extra copies
   */
  public void writeUtf8CharSeq(Utf8CharSequence utf8) throws IOException {
    if (utf8 instanceof ByteArrayUtf8CharSequence) {
      out.write(((ByteArrayUtf8CharSequence) utf8).getBuf());
    }
    utf8.write(out);
  }
}
