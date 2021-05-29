package org.apache.solr.common.util;

import org.agrona.concurrent.MappedResizeableBuffer;

import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

public class DirectMemBufferedInputStream extends InputStream implements DataInputInputStream, DataInput {

    private MappedResizeableBuffer buffer;
    private int offset;
    private int length;
    private long position;

    public DirectMemBufferedInputStream(final MappedResizeableBuffer buffer) {
     this.buffer = buffer;
    }

    @Override
    public int read() throws IOException {
        int b = -1;

        b = buffer.getByte(offset + position) & 0xFF;
        ++position;

        return b  & 0xff;
    }


    public int read(final byte[] dstBytes, final int dstOffset, final int length)
    {
        int bytesRead = length;

      //  if (position < this.length)
      //  {
      //      bytesRead = Math.min(length, available());
            buffer.getBytes(offset + position, dstBytes, dstOffset, length);
            position += bytesRead;
      //  }

        return bytesRead;
    }

    public int available()
    {
        return (int) (length - position);
    }

    /**
     * The offset within the underlying buffer at which to start.
     *
     * @return offset within the underlying buffer at which to start.
     */
    public int offset()
    {
        return offset;
    }

    /**
     * The length of the underlying buffer to use
     *
     * @return length of the underlying buffer to use
     */
    public int length()
    {
        return length;
    }

    /**
     * The underlying buffer being wrapped.
     *
     * @return the underlying buffer being wrapped.
     */
    public MappedResizeableBuffer buffer()
    {
        return buffer;
    }

    public int position()
    {
        return (int) position;
    }

    public void position(long pos)
    {
        this.position = pos;
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
        return (int) super.skip(n);
    }

    @Override
    public boolean readBoolean() throws IOException {
        return false;
    }

    @Override
    public byte readByte() throws IOException {
        int ch = read();
        if (ch < 0)
            throw new EOFException("" + ch);
        return (byte)(ch);
    }

    @Override
    public int readUnsignedByte() throws IOException {
        int ch = read()  & 0xff;
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
        return 0;
    }

    public int readInt() throws IOException {

        return  ((readUnsignedByte() << 24)
                |(readUnsignedByte() << 16)
                |(readUnsignedByte() << 8)
                | readUnsignedByte());
     //  return buffer.getInt(position++);
    }

    public int getInt() throws IOException {
      return buffer.getInt(position++);
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
        return null;
    }

    @Override
    public String readUTF() throws IOException {
        return null;
    }
}
