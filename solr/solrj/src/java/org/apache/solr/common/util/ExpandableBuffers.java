package org.apache.solr.common.util;

import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.io.ExpandableDirectBufferOutputStream;

import java.nio.ByteBuffer;

public class ExpandableBuffers {
  public final static ThreadLocal<ExpandableDirectBufferOutputStream> buffer1 = new ThreadLocal<>() {

    protected ExpandableDirectBufferOutputStream initialValue() {
          MutableDirectBuffer expandableBuffer1 = new ExpandableDirectByteBuffer(8192);
          ExpandableDirectBufferOutputStream stream1 = new ExpandableDirectBufferOutputStream(expandableBuffer1);
          return stream1;
    }

    public ExpandableDirectBufferOutputStream get() {
      ExpandableDirectBufferOutputStream stream1 = super.get();
      return getExpandableDirectBufferOutputStream(stream1, buffer1);
    }
  };

  public final static ThreadLocal<ExpandableDirectBufferOutputStream> buffer2 = new ThreadLocal<>() {

    protected ExpandableDirectBufferOutputStream initialValue() {
      MutableDirectBuffer expandableBuffer1 = new ExpandableDirectByteBuffer(8192);
      ExpandableDirectBufferOutputStream stream1 = new ExpandableDirectBufferOutputStream(expandableBuffer1);
      return stream1;
    }

    public ExpandableDirectBufferOutputStream get() {
      ExpandableDirectBufferOutputStream stream1 = super.get();
      return getExpandableDirectBufferOutputStream(stream1, buffer2);
    }
  };

  private static ExpandableDirectBufferOutputStream getExpandableDirectBufferOutputStream(ExpandableDirectBufferOutputStream stream1,
      ThreadLocal<ExpandableDirectBufferOutputStream> tl) {

    MutableDirectBuffer buffer = stream1.buffer();
    ByteBuffer byteBuffer = buffer.byteBuffer();

    if (byteBuffer.capacity() > 64000) {
      tl.remove();
      return tl.get();
    }
    byteBuffer.clear();
    stream1.wrap(buffer);
    return stream1;
  }


  static {
    SolrQTP.registerThreadLocal(buffer1);
    SolrQTP.registerThreadLocal(buffer2);
  }
}
