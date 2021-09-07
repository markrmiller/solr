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

import org.eclipse.jetty.io.RuntimeIOException;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

/** Single threaded buffered InputStream
 *  Internal Solr use only, subject to change.
 */
public class AlreadyBufferedInputStream extends FastInputStream {
  protected final InputStream in;

  protected int pos;
  protected int end;
  protected long readFromStream; // number of bytes read from the underlying inputstream

  public AlreadyBufferedInputStream(InputStream in) {
    super(null);
    if (in instanceof FastInputStream) {
      throw new IllegalArgumentException();
    }
    this.in = in;
  }


  @Override
  boolean readDirectUtf8(ByteArrayUtf8CharSequence utf8, int len) {
    try {
      utf8.reset(readNBytes(len), 0, len, null);
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
    pos = pos + len;
    return true;
  }

  public static AlreadyBufferedInputStream wrap(InputStream in) {
    return (in instanceof AlreadyBufferedInputStream) ? (AlreadyBufferedInputStream)in : new AlreadyBufferedInputStream(in);
  }

  @Override
  public int read() throws IOException {

    return in.read();
  }

  public int peek() throws IOException {
    throw new UnsupportedOperationException();
  }


  @Override
  public int readUnsignedByte() throws IOException {
    return in.read() & 0xff;
  }

  public int readWrappedStream(byte[] target, int offset, int len) throws IOException {
    return in.read(target, offset, len);
  }

  public long position() {
    throw new UnsupportedOperationException();
    //return readFromStream - (end - pos);
  }

  @Override
  public int available() throws IOException {
    return in.available();
  }

  @Override
  public int read(byte b[], int off, int len) throws IOException {
    return in.read(b, off, len);
  }

  @Override
  public void close() throws IOException {
    in.close();
  }

  @Override
  public void readFully(byte b[]) throws IOException {
    readFully(b, 0, b.length);
  }

  @Override
  public void readFully(byte b[], int off, int len) throws IOException {
    while (len>0) {
      int ret = read(b, off, len);
      if (ret==-1) {
        throw new EOFException();
      }
      off += ret;
      len -= ret;
    }
  }

  @Override
  public int skipBytes(int n) throws IOException {
    return (int) in.skip(n);
  }

  @Override
  public boolean readBoolean() throws IOException {
    return readByte()==1;
  }

  @Override
  public byte readByte() throws IOException {
    return (byte) in.read();
  }


  @Override
  public short readShort() throws IOException {
    return (short)((readUnsignedByte() << 8) | readUnsignedByte());
  }

  @Override
  public int readUnsignedShort() throws IOException {
    return (readUnsignedByte() << 8) | readUnsignedByte();
  }

  @Override
  public char readChar() throws IOException {
    return (char)((readUnsignedByte() << 8) | readUnsignedByte());
  }

  @Override
  public int readInt() throws IOException {
    return  ((readUnsignedByte() << 24)
            |(readUnsignedByte() << 16)
            |(readUnsignedByte() << 8)
            | readUnsignedByte());
  }

  @Override
  public long readLong() throws IOException {
    return  (((long)readUnsignedByte()) << 56)
            | (((long)readUnsignedByte()) << 48)
            | (((long)readUnsignedByte()) << 40)
            | (((long)readUnsignedByte()) << 32)
            | (((long)readUnsignedByte()) << 24)
            | (readUnsignedByte() << 16)
            | (readUnsignedByte() << 8)
            | (readUnsignedByte());
  }

  @Override
  public float readFloat() throws IOException {
    return Float.intBitsToFloat(readInt());    
  }

  @Override
  public double readDouble() throws IOException {
    return Double.longBitsToDouble(readLong());    
  }

  @Override
  public String readLine() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public String readUTF() throws IOException {
    return new DataInputStream(this).readUTF();
  }
}
