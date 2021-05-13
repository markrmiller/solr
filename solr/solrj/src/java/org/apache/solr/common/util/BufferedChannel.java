package org.apache.solr.common.util;

import org.agrona.BitUtil;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;

import java.io.Closeable;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;

public class BufferedChannel extends OutputStream implements Closeable {

    private final FileChannel ch;
    private final MutableDirectBuffer buff;
    private int pos;

    protected int count;
    protected int size;



    /**
     * Creates a new buffered output stream to write data to the
     * specified underlying output stream with the specified buffer
     * size.
     *
     * @param   out    the underlying output stream.
     * @param   size   the buffer size.
     * @exception IllegalArgumentException if size &lt;= 0.
     */
    public BufferedChannel(FileChannel out, int size) {
        ch = out;
        buff = ExpandableBuffers.getInstance().acquire(-1, true);
        pos = 0;
        buff.byteBuffer().limit(buff.byteBuffer().capacity());
    }

    /** Flush the internal buffer */
    public void flushBuffer() throws IOException {
        if (count > 0) {
            ByteBuffer bb = buff.byteBuffer();
            bb.limit(count);
            bb.position(0);
        //    BufferUtil.flipToFlush(bb, pos);
            ch.write(bb);
       //     pos = BufferUtil.flipToFill(buff.byteBuffer());
            bb.position(0);
            bb.limit(bb.capacity());
            count = 0;
        }
    }

    /**
     * Writes the specified byte to this buffered output stream.
     *
     * @param      b   the byte to be written.
     * @exception  IOException  if an I/O error occurs.
     */
    @Override
    public void write(int b) throws IOException {
        if (count >= buff.byteBuffer().remaining()) {
            flushBuffer();
        }
        buff.putByte(count++, (byte)b);
        size++;
    }

    @Override
    public void write(byte b[], int off, int len) throws IOException {
//        if (len >= buff.byteBuffer().remaining()) {
//            /* If the request length exceeds the size of the output buffer,
//               flush the output buffer and then write the data directly.
//               In this way buffered streams will cascade harmlessly. */
//            flushBuffer();
//            ch.write(buff.byteBuffer());
//            return;
//        }
        if (len >= buff.byteBuffer().remaining()) {
            flushBuffer();
//            ch.write(ByteBuffer.wrap(b, off, len));
//            return;
        }

        buff.putBytes(count, b, off, len);
        count += len;
        size += len;
    }

    @Override
    public void flush() throws IOException {
        flushBuffer();
    }

    public long size() {
        return size;
//        try {
//            return ch.size();
//        } catch (IOException ioException) {
//           throw new RuntimeIOException(ioException);
//        }
    }

    public void writeInt(int v) {
       // buff.putInt(count, i);
        buff.putByte(count++, (byte) ((v >>> 24) & 0xFF));
        buff.putByte(count++, (byte) ((v >>> 16) & 0xFF));
        buff.putByte(count++, (byte) ((v >>>  8) & 0xFF));
        buff.putByte(count++, (byte) ((v >>>  0) & 0xFF));
      //  count+=BitUtil.SIZE_OF_INT;
        size+=BitUtil.SIZE_OF_INT;
    }

    public void setWritten(int start) {
      size = start;
    }

    public void close() throws IOException {
        flushBuffer();
        ch.close();
        ExpandableBuffers.getInstance().release(buff);
    }
}
