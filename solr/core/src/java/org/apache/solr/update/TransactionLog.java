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
package org.apache.solr.update;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.apache.solr.common.util.*;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.eclipse.jetty.io.RuntimeIOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  Log Format: List{Operation, Version, ...}
 *  ADD, VERSION, DOC
 *  DELETE, VERSION, ID_BYTES
 *  DELETE_BY_QUERY, VERSION, String
 *
 *  TODO: keep two files, one for [operation, version, id] and the other for the actual
 *  document data.  That way we could throw away document log files more readily
 *  while retaining the smaller operation log files longer (and we can retrieve
 *  the stored fields from the latest documents from the index).
 *
 *  This would require keeping all source fields stored of course.
 *
 *  This would also allow to not log document data for requests with commit=true
 *  in them (since we know that if the request succeeds, all docs will be committed)
 *
 */
public class TransactionLog implements Closeable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final boolean debug = log.isDebugEnabled();
  private final boolean trace = log.isTraceEnabled();

  public final static String END_MESSAGE = "SOLR_TLOG_END";

  long id;
  volatile File tlogFile;
  RandomAccessFile raf;
  FileChannel channel;
 // OutputStream os;
  BufferedChannel fos;    // all accesses to this stream should be synchronized on "this" (The TransactionLog)

  //GetChannelInputStream cis;

  final ReentrantLock fosLock = new ReentrantLock(true);
  private final LongAdder numRecords = new LongAdder();
  boolean isBuffer;

  protected volatile boolean deleteOnClose = true;  // we can delete old tlogs since they are currently only used for real-time-get (and in the future, recovery)

  AtomicInteger refcount = new AtomicInteger(1);


  Object2IntMap globalStringMap = new Object2IntOpenHashMap();
  List<String> globalStringList = new ArrayList<>();

  // write a BytesRef as a byte array
  static final JavaBinCodec.ObjectResolver resolver = (o, codec) -> {
    if (o instanceof BytesRef) {
      BytesRef br = (BytesRef)o;
      codec.writeByteArray(br.bytes, br.offset, br.length);
      return null;
    }
    // Fallback: we have no idea how to serialize this.  Be noisy to prevent insidious bugs
    throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "TransactionLog doesn't know how to serialize " + o.getClass() + "; try implementing ObjectResolver?");
  };

  public class LogCodec extends JavaBinCodec {

    public LogCodec(JavaBinCodec.ObjectResolver resolver) {
      super(resolver);
    }

    @Override
    public void writeExternString(CharSequence s) throws IOException {
      if (s == null) {
        writeTag(NULL);
        return;
      }

      // no need to synchronize globalStringMap - it's only updated before the first record is written to the log
    //  Integer idx = globalStringMap.getInt(s.toString());
    //  if (idx == null) {
        // write a normal string
        writeStr(s, false);
    //  } else {
        // write the extern string
   //     writeTag(EXTERN_STRING, idx);
   //   }
    }

    @Override
    public CharSequence readExternString(DataInputInputStream fis) throws IOException {
      int idx = readVInt(fis);//readSize(fis);
   //   if (idx != 0) {// idx != 0 is the index of the extern string
        // no need to synchronize globalStringList - it's only updated before the first record is written to the log
    //    return globalStringList.get(idx - 1);
    //  } else {// idx == 0 means it has a string value
        // this shouldn't happen with this codec subclass.
        return readStr(fis, null, false);
   //   }
    }


    @Override
    protected Object readObject(DataInputInputStream dis) throws IOException {
      if (UUID == tagByte) {
        return new java.util.UUID(dis.readLong(), dis.readLong());
      }
      return super.readObject(dis);
    }

    @Override
    public boolean writePrimitive(Object val) throws IOException {
      if (val instanceof java.util.UUID) {
        java.util.UUID uuid = (java.util.UUID) val;
        daos.write(UUID);
        writeLongToOut(uuid.getMostSignificantBits());
        writeLongToOut(uuid.getLeastSignificantBits());
        return true;
      }
      return super.writePrimitive(val);
    }
  }

  TransactionLog(File tlogFile, Collection<String> globalStrings) {
    this(tlogFile, globalStrings, false);
  }

  TransactionLog(File tlogFile, Collection<String> globalStrings, boolean openExisting) {
    boolean success = false;
    try {
      if (debug) {
        log.debug("New TransactionLog file= {}, exists={}, size={} openExisting={}"
                , tlogFile, tlogFile.exists(), tlogFile.length(), openExisting);
      }

      // Parse tlog id from the filename
      String filename = tlogFile.getName();
      id = Long.parseLong(filename.substring(filename.lastIndexOf('.') + 1));

      this.tlogFile = tlogFile;
      raf = new RandomAccessFile(this.tlogFile, "rw");
      long start = raf.length();
      channel = raf.getChannel();
      //os = Channels.newOutputStream(channel);
      fos = new BufferedChannel(channel, 8192);
     // cis = new GetChannelInputStream(channel, Channels.newInputStream(channel));

      if (openExisting) {
        if (start > 0) {
          readHeader(null);
          raf.seek(start);
          assert channel.position() == start;
          fos.setWritten((int) start);    // reflect that we aren't starting at the beginning
          assert fos.size() == channel.size();
        } else {
          addGlobalStrings(globalStrings);
        }
      } else {
        if (start > 0) {
          log.warn("New transaction log already exists:{} size={}", tlogFile, raf.length());
          return;
        }

        addGlobalStrings(globalStrings);
      }

      success = true;

      // TODO: like updatelog, this is currently very hard to nail 1000% as
      // updates can spawn a new one,.l
      // assert ObjectReleaseTracker.track(this);

    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    } finally {
      if (!success && raf != null) {
        try {
          raf.close();
        } catch (Exception e) {
          log.error("Error closing tlog file (after error opening)", e);
        }
      }
    }
  }

  // for subclasses
  protected TransactionLog() {}

  /** Returns the number of records in the log (currently includes the header and an optional commit).
   * Note: currently returns 0 for reopened existing log files.
   */
  public int numRecords() {
    return this.numRecords.intValue();
  }

  public boolean endsWithCommit() throws IOException {
    long size;
    fosLock.lock();
    try {
      fos.flush();
      size = fos.size();
    } finally {
      fosLock.unlock();
    }

    // the end of the file should have the end message (added during a commit) plus a 4 byte size
    byte[] buf = new byte[END_MESSAGE.length()];
    long pos = size - END_MESSAGE.length() - 4;
    if (pos < 0) return false;
    final FastInputStream is = new ChannelFastInputStream(channel,  new GetChannelInputStream(channel, Channels.newInputStream(channel)), pos);
    is.read(buf);
    for (int i = 0; i < buf.length; i++) {
      if (buf[i] != END_MESSAGE.charAt(i)) return false;
    }
    return true;
  }

  @SuppressWarnings({"unchecked"})
  private void readHeader(FastInputStream fis) throws IOException {
    // read existing header
   // fis = fis != null ? fis : new ChannelFastInputStream(channel,  new GetChannelInputStream(channel, Channels.newInputStream(channel)), 0);

    @SuppressWarnings("resource") final LogCodec codec = new LogCodec(resolver);

    fis.position(0);
    fis.flush();

    @SuppressWarnings({"rawtypes"})
    Map header = (Map) codec.unmarshal(fis);


    fis.readInt(); // skip size

    // needed to read other records

    fosLock.lock();
    try {
      globalStringList = (List<String>) header.get("strings");
      globalStringMap = new Object2IntOpenHashMap(globalStringList.size());
      for (int i = 0; i < globalStringList.size(); i++) {
        globalStringMap.put(globalStringList.get(i), i + 1);
      }
    } finally {
      fosLock.unlock();
    }
  }

  protected void addGlobalStrings(Collection<String> strings) {
    if (strings == null) return;
    fosLock.lock();
    try {
      int origSize = globalStringMap.size();
      for (String s : strings) {
        Integer idx = null;
        if (origSize > 0) {
          idx = globalStringMap.getInt(s);
        }
        if (idx != null) continue;  // already in list
        globalStringList.add(s);
        globalStringMap.put(s, globalStringList.size());
      }
      assert globalStringMap.size() == globalStringList.size();
    } finally {
      fosLock.unlock();
    }
  }

  Collection<String> getGlobalStrings() {
    fosLock.lock();
    try {
      return new ArrayList<>(globalStringList);
    } finally {
      fosLock.unlock();
    }
  }

  @SuppressWarnings({"unchecked"})
  protected void writeLogHeader(LogCodec codec) throws IOException {
    long pos = fos.size();
    assert pos == 0;

    @SuppressWarnings({"rawtypes"})
    Map header = new LinkedHashMap<String, Object>(2);
    header.put("SOLR_TLOG", 1); // a magic string + version number
    header.put("strings", globalStringList);
    codec.marshal(header, fos);

    endRecord(pos);
    fos.flushBuffer();
  }

  protected void endRecord(long startRecordPosition) throws IOException {
    fos.writeInt((int) (fos.size() - startRecordPosition));
    numRecords.increment();
  }

  protected void checkWriteHeader(LogCodec codec, SolrInputDocument optional) throws IOException {

    // Unsynchronized access. We can get away with an unsynchronized access here
    // since we will never get a false non-zero when the position is in fact 0.
    // rollback() is the only function that can reset to zero, and it blocks updates.
    if (fos.size() != 0) return;

    fosLock.lock();
    try {
      if (fos.size() != 0) return;  // check again while synchronized
      if (optional != null) {
        addGlobalStrings(optional.getFieldNames());
      }
      writeLogHeader(codec);
    } finally {
      fosLock.unlock();
    }
  }

  volatile int lastAddSize;

  /**
   * Writes an add update command to the transaction log. This is not applicable for
   * in-place updates; use {@link #write(AddUpdateCommand, long)}.
   * (The previous pointer (applicable for in-place updates) is set to -1 while writing
   * the command to the transaction log.)
   * @param cmd The add update command to be written
   * @return Returns the position pointer of the written update command
   *
   * @see #write(AddUpdateCommand, long)
   */
  public long write(AddUpdateCommand cmd) {
    return write(cmd, -1);
  }

  /**
   * Writes an add update command to the transaction log. This should be called only for
   * writing in-place updates, or else pass -1 as the prevPointer.
   * @param cmd The add update command to be written
   * @param prevPointer The pointer in the transaction log which this update depends
   * on (applicable for in-place updates)
   * @return Returns the position pointer of the written update command
   */
  public long write(AddUpdateCommand cmd, long prevPointer) {
    assert (-1 <= prevPointer && (cmd.isInPlaceUpdate() || (-1 == prevPointer)));

    LogCodec codec = new LogCodec(resolver);
    SolrInputDocument sdoc = cmd.getSolrInputDocument();

    try {
      checkWriteHeader(codec, sdoc);

      // adaptive buffer sizing
      int bufSize = lastAddSize;
      // at least 256 bytes and at most 1 MB
      bufSize = Math.min(1024 * 1024, Math.max(256, bufSize + (bufSize >> 3) + 256));

      MutableDirectBuffer expandableBuffer1 = ExpandableBuffers.getInstance().acquire(-1, true);
      try {
        ExpandableDirectBufferOutputStream out = new ExpandableDirectBufferOutputStream(expandableBuffer1);

        codec.init(out);
        if (cmd.isInPlaceUpdate()) {
          codec.writeTag(JavaBinCodec.ARR, 5);
          codec.writeInt(UpdateLog.UPDATE_INPLACE);  // should just take one byte
          codec.writeLong(cmd.getVersion());
          codec.writeLong(prevPointer);
          codec.writeLong(cmd.prevVersion);
          codec.writeSolrInputDocument(cmd.getSolrInputDocument());
        } else {
          codec.writeTag(JavaBinCodec.ARR, 3);
          codec.writeInt(UpdateLog.ADD);  // should just take one byte
          codec.writeLong(cmd.getVersion());
          codec.writeSolrInputDocument(cmd.getSolrInputDocument());
        }


        fosLock.lock();
        try {


          long pos = fos.size();   // if we had flushed, this should be equal to channel.position()

          lastAddSize = out.position();

          assert pos != 0;

        /*
         System.out.println("###writing at " + pos + " fos.size()=" + fos.size() + " raf.length()=" + raf.length());
         if (pos != fos.size()) {
         throw new RuntimeException("ERROR" + "###writing at " + pos + " fos.size()=" + fos.size() + " raf.length()=" + raf.length());
         }
         */
          ByteBuffer buffer = out.buffer().byteBuffer().asReadOnlyBuffer();
          buffer.position(out.offset() + out.buffer().wrapAdjustment());
          buffer.limit(out.position() + out.buffer().wrapAdjustment());
          fos.flushBuffer();
          channel.write(buffer);
          // fos.flushBuffer();


          endRecord(pos);

          return pos;
        } finally {
          fosLock.unlock();
        }
      } finally {
        ExpandableBuffers.getInstance().release(expandableBuffer1);
      }

    } catch (IOException e) {
      // TODO: reset our file pointer back to "pos", the start of this record.
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error logging add", e);
    }
  }

  public long writeDelete(DeleteUpdateCommand cmd) {
    LogCodec codec = new LogCodec(resolver);

    try {
      checkWriteHeader(codec, null);

      BytesRef br = cmd.getIndexedId();


      MutableDirectBuffer expandableBuffer1 = new ExpandableArrayBuffer(20 + (br.length));

      ExpandableDirectBufferOutputStream out = new ExpandableDirectBufferOutputStream(expandableBuffer1);
      codec.init(out);
      codec.writeTag(JavaBinCodec.ARR, 3);
      codec.writeInt(UpdateLog.DELETE);  // should just take one byte
      codec.writeLong(cmd.getVersion());
      codec.writeByteArray(br.bytes, br.offset, br.length);

      fosLock.lock();
      try {

        fos.flushBuffer();
        channel.write(ByteBuffer.wrap(out.buffer().byteArray(), 0, out.position() + expandableBuffer1.wrapAdjustment()));
        long pos = fos.size();   // if we had flushed, this should be equal to channel.position()
        assert pos != 0;
        endRecord(pos);
        // fos.flushBuffer();  // flush later
        return pos;
      } finally {
        fosLock.unlock();
      }

    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }

  }

  public long writeDeleteByQuery(DeleteUpdateCommand cmd) {
    LogCodec codec = new LogCodec(resolver);
    try {
      checkWriteHeader(codec, null);
   //   long initSize = fos.size();
      MutableDirectBuffer expandableBuffer1 = new ExpandableArrayBuffer(20 + (cmd.query.length()));

      ExpandableDirectBufferOutputStream out = new ExpandableDirectBufferOutputStream(expandableBuffer1);

      codec.init(out);
      codec.writeTag(JavaBinCodec.ARR, 3);
      codec.writeInt(UpdateLog.DELETE_BY_QUERY);  // should just take one byte
      codec.writeLong(cmd.getVersion());
      codec.writeStr(cmd.query, false);

      fosLock.lock();
      try {

        fos.flushBuffer();

        channel.write(ByteBuffer.wrap(out.buffer().byteArray(), 0, out.position() + expandableBuffer1.wrapAdjustment()));
        //out.writeTo(fos);
        long pos = fos.size();   // if we had flushed, this should be equal to channel.position()
        endRecord(pos);
        // fos.flushBuffer();  // flush later
        return pos;
      } finally {
        fosLock.unlock();
      }
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }

  }


  public long writeCommit(CommitUpdateCommand cmd) {
    LogCodec codec = new LogCodec(resolver);
    fosLock.lock();
    try {
      try {
        long pos = fos.size();   // if we had flushed, this should be equal to channel.position()

        if (pos == 0) {
          writeLogHeader(codec);
        }

        MutableDirectBuffer expandableBuffer1 = new ExpandableArrayBuffer(32); // MRM TODO:

        ExpandableDirectBufferOutputStream out = new ExpandableDirectBufferOutputStream(expandableBuffer1);
        codec.init(out);
        codec.writeTag(JavaBinCodec.ARR, 3);
        codec.writeInt(UpdateLog.COMMIT);  // should just take one byte
        codec.writeLong(cmd.getVersion());
        codec.writeStr(END_MESSAGE, false);  // ensure these bytes are (almost) last in the file
        codec.writeInt(out.position());
        fos.flush();  // flush since this will be the last record in a log fill
        channel.write(ByteBuffer.wrap(out.buffer().byteArray(), 0, out.position() + expandableBuffer1.wrapAdjustment()));

        numRecords.increment();

        assert fos.size() == channel.size() : "fos="+fos.size() + " ch=" + channel.size();
        pos = channel.size();
        return pos;
      } catch (IOException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      }
    } finally {
      fosLock.unlock();
    }
  }


  /* This method is thread safe */

  public Object lookup(long pos) {
    // A negative position can result from a log replay (which does not re-log, but does
    // update the version map.  This is OK since the node won't be ACTIVE when this happens.
    if (pos < 0) return null;

    try {
      // make sure any unflushed buffer has been flushed
      fosLock.lock();
      try {
        // TODO: optimize this by keeping track of what we have flushed up to
        fos.flushBuffer();
        /***
         System.out.println("###flushBuffer to " + fos.size() + " raf.length()=" + raf.length() + " pos="+pos);
         if (fos.size() != raf.length() || pos >= fos.size() ) {
         throw new RuntimeException("ERROR" + "###flushBuffer to " + fos.size() + " raf.length()=" + raf.length() + " pos="+pos);
         }
         ***/
      } finally {
        fosLock.unlock();
      }

      FastInputStream fis = new ChannelFastInputStream(channel,  new GetChannelInputStream(channel, Channels.newInputStream(channel)), pos);
      try (LogCodec codec = new LogCodec(resolver)) {
        return codec.readVal(fis);
      }
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  public void incref() {
    refcount.updateAndGet(operand -> {
      if (operand == 0) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "incref on a closed log: " + this);
      }
      return operand + 1;
    });
  }

  public boolean tryIncref() {
    AtomicBoolean result = new AtomicBoolean();
    refcount.updateAndGet(operand -> {
      if (operand == 0) {
        result.set(false);
        return 0;
      }
      result.set(true);
      return operand + 1;
    });

    return result.get();
  }

  public void decref() {
     refcount.updateAndGet(operand -> {
      if (operand == 0) {
        return 0;
      }
      if (operand == 1) {
        close();
      }
      return  operand - 1;
    });
  }

  /** returns the current position in the log file */
  public long position() {
    fosLock.lock();
    try {
      return fos.size();
    } finally {
      fosLock.unlock();
    }
  }

  /** Move to a read-only state, closing and releasing resources while keeping the log available for reads */
  public void closeOutput() {

  }

  public void finish(UpdateLog.SyncLevel syncLevel) {
    if (syncLevel == UpdateLog.SyncLevel.NONE) return;
    try {
      fosLock.lock();
      try {
        fos.flushBuffer();
      } finally {
        fosLock.unlock();
      }

      if (syncLevel == UpdateLog.SyncLevel.FSYNC) {
        // Since fsync is outside of synchronized block, we can end up with a partial
        // last record on power failure (which is OK, and does not represent an error...
        // we just need to be aware of it when reading).
        raf.getFD().sync();
      }

    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  public void close() {
    try {
      if (debug) {
        log.debug("Closing tlog {}", this);
      }

      fosLock.lock();
      try {
        fos.close();
      } finally {
        fosLock.unlock();
      }

      if (deleteOnClose) {
        try {
          Files.deleteIfExists(tlogFile.toPath());
        } catch (IOException e) {
          // TODO: should this class care if a file couldnt be deleted?
          // this just emulates previous behavior, where only SecurityException would be handled.
        }
      }
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    } finally {
      assert ObjectReleaseTracker.getInstance().release(this);
    }
  }

  public void forceClose() {
    if (refcount.get() > 0) {
      log.error("Error: Forcing close of {}", this);
      refcount.set(0);
      close();
    }
  }

  @Override
  public String toString() {
    try {
      return "tlog{file=" + tlogFile.toString() + " refcount=" + refcount.get() + " size=" + channel.size() + "}";
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
  }

  public long getLogSize() {
    if (tlogFile != null) {
      return tlogFile.length();
    }
    return 0;
  }

  /**
   * @return the FastOutputStream size
   */
  public long getLogSizeFromStream() {
    fosLock.lock();
    try {
      return fos.size();
    } finally {
      fosLock.unlock();
    }
  }

  /** Returns a reader that can be used while a log is still in use.
   * Currently only *one* LogReader may be outstanding, and that log may only
   * be used from a single thread. */
  public LogReader getReader(long startingPos) {
    return new LogReader(startingPos);
  }

  public LogReader getSortedReader(long startingPos) {
    return new SortedLogReader(startingPos);
  }

  /** Returns a single threaded reverse reader */
  public ReverseReader getReverseReader() throws IOException {
    return new FSReverseReader();
  }

  public class LogReader {
    protected ChannelFastInputStream fis;
    private final LogCodec codec = new LogCodec(resolver);

    public LogReader(long startingPos) {
      incref();

      fis = new ChannelFastInputStream(channel,  new GetChannelInputStream(channel, Channels.newInputStream(channel)), startingPos);

    }

    // for classes that extend
    protected LogReader() {}

    /** Returns the next object from the log, or null if none available.
     *
     * @return The log record, or null if EOF
     * @throws IOException If there is a low-level I/O error.
     */
    public Object next() throws IOException, InterruptedException {
      long pos;
      fosLock.lock();
      try {
        pos = fis.position();
        if (trace) {
          log.trace("Reading log record.  pos={} currentSize={}", pos, fos.size());
        }

        if (pos >= fos.size()) {
          return null;
        }

        fos.flushBuffer();
      } finally {
        fosLock.unlock();
      }
      if (pos == 0) {
        readHeader(fis);

        // shouldn't currently happen - header and first record are currently written at the same time
        fosLock.lock();
        try {
          if (fis.position() >= fos.size()) {
            return null;
          }
          pos = fis.position();
        } finally {
          fosLock.unlock();
        }
      }

      Object o = codec.readVal(fis);

      // skip over record size
      int size = fis.readInt();
      assert size == fis.position() - pos - 4;

      return o;
    }

    public void close() {
      decref();
    }

    @Override
    public String toString() {
      synchronized (TransactionLog.this) {
        try {
          return "LogReader{" + "file=" + tlogFile + ", position=" + fis.position() + ", end=" + fos.size() + "}";
        } catch (IOException e) {
          throw new RuntimeIOException(e);
        }
      }
    }

    public long currentPos() {
      try {
        return fis.position();
      } catch (IOException e) {
        throw new RuntimeIOException(e);
      }
    }

    // returns best effort current size
    // for info purposes
    public long currentSize() throws IOException {
      return channel.size();
    }

  }

  public class SortedLogReader extends LogReader {
    private final long startingPos;
    private boolean inOrder = true;
    private TreeMap<Long, Long> versionToPos;
    Iterator<Long> iterator;

    public SortedLogReader(long startingPos) {
      super(startingPos);
      this.startingPos = startingPos;
    }

    @Override
    public Object next() throws IOException, InterruptedException {
      if (versionToPos == null) {
        versionToPos = new TreeMap<>();
        Object o;
        long pos = startingPos;

        long lastVersion = Long.MIN_VALUE;
        while ((o = super.next()) != null) {
          @SuppressWarnings({"rawtypes"})
          List entry = (List) o;
          long version = (Long) entry.get(UpdateLog.VERSION_IDX);
          version = Math.abs(version);
          versionToPos.put(version, pos);
          pos = currentPos();

          if (version < lastVersion) inOrder = false;
          lastVersion = version;
        }
        fis.position(startingPos);
      }

      if (inOrder) {
        return super.next();
      } else {
        if (iterator == null) iterator = versionToPos.values().iterator();
        if (!iterator.hasNext()) return null;
        long pos = iterator.next();
        if (pos != currentPos()) fis.position(pos);
        return super.next();
      }
    }
  }

  public abstract static class ReverseReader {

    /** Returns the next object from the log, or null if none available.
     *
     * @return The log record, or null if EOF
     * @throws IOException If there is a low-level I/O error.
     */
    public abstract Object next() throws IOException;

    /* returns the position in the log file of the last record returned by next() */
    public abstract long position();

    public abstract void close();

    @Override
    public abstract String toString();

  }

  public class FSReverseReader extends ReverseReader {
    ChannelFastInputStream fis;
    private final LogCodec codec = new LogCodec(resolver) {
      @Override
      public SolrInputDocument readSolrInputDocument(DataInputInputStream dis) {
        // Given that the SolrInputDocument is last in an add record, it's OK to just skip
        // reading it completely.
        return null;
      }
    };

    int nextLength;  // length of the next record (the next one closer to the start of the log file)
    long prevPos;    // where we started reading from last time (so prevPos - nextLength == start of next record)

    public FSReverseReader() throws IOException {
      incref();

      long sz;
      fosLock.lock();
      try {
        fos.flushBuffer();
        sz = fos.size();
        assert sz == channel.size() : "sz:" + sz + " ch:" + channel.size();
      } finally {
        fosLock.unlock();
      }

        fis = new ChannelFastInputStream(channel,  new GetChannelInputStream(channel, Channels.newInputStream(channel)), 0);
        if (sz >= 4) {
          // readHeader(fis);  // should not be needed
          prevPos = sz - 4;
          fis.position(prevPos);
          nextLength = fis.readInt();
        }
    }

    /** Returns the next object from the log, or null if none available.
     *
     * @return The log record, or null if EOF
     * @throws IOException If there is a low-level I/O error.
     */
    public Object next() throws IOException {
      if (prevPos <= 0) return null;

      long endOfThisRecord = prevPos;

      int thisLength = nextLength;

      long recordStart = prevPos - thisLength;  // back up to the beginning of the next record
      prevPos = recordStart - 4;  // back up 4 more to read the length of the next record

      if (prevPos <= 0) return null;  // this record is the header

      long bufferPos = fis.getPositionInBuffer();
      if (prevPos >= bufferPos) {
        // nothing to do... we're within the current buffer
      } else {
        // Position buffer so that this record is at the end.
        // For small records, this will cause subsequent calls to next() to be within the buffer.
        long seekPos = endOfThisRecord - fis.getBufferSize();
        seekPos = Math.min(seekPos, prevPos); // seek to the start of the record if it's larger then the block size.
        seekPos = Math.max(seekPos, 0);
        fis.position(seekPos);
        fis.peek();  // cause buffer to be filled
      }

      fis.position(prevPos);
      nextLength = fis.readInt();     // this is the length of the *next* record (i.e. closer to the beginning)

      // TODO: optionally skip document data

      // assert fis.position() == prevPos + 4 + thisLength;  // this is only true if we read all the data (and we currently skip reading SolrInputDocument

      return codec.readVal(fis);
    }

    /* returns the position in the log file of the last record returned by next() */
    public long position() {
      return prevPos + 4;  // skip the length
    }

    public void close() {
      decref();
    }

    @Override
    public String toString() {
      fosLock.lock();
      try {
        return "LogReader{" + "file=" + tlogFile + ", position=" + fis.position() + ", end=" + fos.size() + "}";
      } catch (IOException e) {
        throw new RuntimeIOException(e);
      } finally {
        fosLock.unlock();
      }
    }


  }

  static class ChannelFastInputStream extends FastInputStream {
    private final FileChannel ch;

    public ChannelFastInputStream(FileChannel ch, InputStream is, long chPosition) {
      // super(null, new byte[10],0,0);    // a small buffer size for testing purposes
  //    super(is, (int) chPosition);
      super(is, (int) chPosition);
      this.ch = ch;
    }

//    public FileChannel getChannel() {
//      return ch;
//    }

//    @Override
//    public int readWrappedStream(byte[] target, int offset, int len) throws IOException {
//      ByteBuffer bb = ByteBuffer.wrap(target, offset, len);
//      return ch.read(bb, readFromStream);
//    }

//    @Override
//    public void readFully(byte b[], int off, int len) throws IOException {
//      ByteBuffer bb = ByteBuffer.wrap(b, off, len);
//      ch.read(bb, readBytes);
//    }

  /** where is the start of the buffer relative to the whole file */
//    public long getBufferPos() {
//      return g;
//    }

    public int getBufferSize() {
      return buffer.length;
    }

    @Override
    public void close() throws IOException {
      //ch.close();
      super.close();
    }

//    @Override
//    public void flush() {
//      ((FastBufferedInputStream) is).flush();
//    }

    @Override
    public String toString() {
      try {
        return "readFromStream=" + readBytes + " pos=" + pos + " bufferPos=" + getPositionInBuffer() + " position=" + position() + " size=" + ch.size();
      } catch (IOException e) {
        throw new RuntimeIOException(e);
      }
    }
  }
}


