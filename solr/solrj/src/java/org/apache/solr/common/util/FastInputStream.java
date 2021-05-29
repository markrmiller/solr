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

import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import org.eclipse.jetty.io.RuntimeIOException;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

/** Single threaded buffered InputStream
 *  Internal Solr use only, subject to change.
 */
public class FastInputStream extends FastBufferedInputStream implements DataInputInputStream, DataInput {
//  public final stati ThreadLocal<byte[]> THREAD_LOCAL_BYTEARRAY= new ThreadLocal<>(){
//    protected byte[] initialValue() {
//      return new byte[8192];
//    }
//  };

 // protected long readFromStream; // number of bytes read from the underlying inputstream

  public FastInputStream(InputStream in) {
    this(in, 0);
  }

  public FastInputStream(InputStream in, int start) {
    super(in);
//    if (start != 0) {
//      try {
//        position(start);
//      } catch (IOException e) {
//        throw new RuntimeIOException(e);
//      }
//    }
    //   flush();
  }

  public static FastInputStream wrap(InputStream in) {
    return (in instanceof FastInputStream) ? (FastInputStream)in : new FastInputStream(in);
  }

  @Override
  public int read() throws IOException {
    return super.read() ;// & 0xff;
  }

  public int peek() throws IOException {
    if (noMoreCharacters()) return -1;
    return buffer[pos];
  }

  /** Returns the internal buffer used for caching */
  public byte[] getBuffer() {
    return buffer;
  }

  @Override
  public long position() throws IOException {
    return readBytes;
   // return super.position();
  }

  /** Current position within the internal buffer */
  public int getPositionInBuffer() {
    return pos;
  }

  /** Current end-of-data position within the internal buffer.  This is one past the last valid byte. */
  public int getEndInBuffer() {
    return pos + avail;
  }


//  @Override
//  public int skipBytes(int n) throws IOException {
//    return (int) skipBytes(n);
//  }

  @Override
  public boolean readBoolean() throws IOException {
    return readByte()==1;
  }

  @Override
  public void close() throws IOException {
    super.close();
  }

  @Override
  public void readFully(byte b[]) throws IOException {
    readFully(b, 0, b.length);
  }

  @Override
  public void readFully(byte b[], int off, int len) throws IOException {
    while (len>0) {
      int ret = super.read(b, off, len);
      if (ret==-1) {
        throw new EOFException();
      }
      off += ret;
      len -= ret;
    }
  }

  @Override
  public int skipBytes(int n) throws IOException {
    return (int) super.skip(n);
  }

  @Override
  public byte readByte() throws IOException {
    int ch = super.read();
    if (ch < 0)
      throw new EOFException("" + ch);
    return (byte)(ch);
  }

  @Override
  public int readUnsignedByte() throws IOException {
    int ch = super.read();
    if (ch < 0)
      throw new EOFException();
    return ch;
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
    throw new UnsupportedOperationException();
  }
}
