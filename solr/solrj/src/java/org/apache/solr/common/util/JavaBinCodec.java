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


import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.io.DirectBufferInputStream;
import org.agrona.io.DirectBufferOutputStream;
import org.apache.solr.common.ConditionalKeyMapWriter;
import org.apache.solr.common.EnumFieldValue;
import org.apache.solr.common.IteratorWriter;
import org.apache.solr.common.IteratorWriter.ItemWriter;
import org.apache.solr.common.MapSerializable;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.PushWriter;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.params.CommonParams;

import org.noggit.CharArr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.apache.solr.common.util.ByteArrayUtf8CharSequence.convertCharSeq;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Defines a space-efficient serialization/deserialization format for transferring data.
 * <p>
 * JavaBinCodec has built in support many commonly used types.  This includes primitive types (boolean, byte,
 * short, double, int, long, float), common Java containers/utilities (Date, Map, Collection, Iterator, String,
 * Object[], byte[]), and frequently used Solr types ({@link NamedList}, {@link SolrDocument},
 * {@link SolrDocumentList}). Each of the above types has a pair of associated methods which read and write
 * that type to a stream.
 * <p>
 * Classes that aren't supported natively can still be serialized/deserialized by providing
 * an {@link JavaBinCodec.ObjectResolver} object that knows how to work with the unsupported class.
 * This allows {@link JavaBinCodec} to be used to marshall/unmarshall arbitrary content.
 * <p>
 * NOTE -- {@link JavaBinCodec} instances cannot be reused for more than one marshall or unmarshall operation.
 */
@SuppressWarnings("unchecked")
public class JavaBinCodec implements PushWriter {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final AtomicBoolean WARNED_ABOUT_INDEX_TIME_BOOSTS = new AtomicBoolean();

  public static final byte
      NULL = 1,
      BOOL_TRUE = 2,
      BOOL_FALSE = 3,
      BYTE = 4,
      SHORT = 5,
      DOUBLE = 6,
      INT = 7,
      LONG = 8,
      FLOAT = 9,
      DATE = 10,
      MAP = 11,
      SOLRDOC = 12,
      SOLRDOCLST = 13,
      BYTEARR = 14,
      ITERATOR = 15,
  /**
   * this is a special tag signals an end. No value is associated with it
   */
  END = 16,

  SOLRINPUTDOC = 17,
      MAP_ENTRY_ITER = 18,
      ENUM_FIELD_VALUE = 19,
      MAP_ENTRY = 20,
      UUID = 21, // This is reserved to be used only in LogCodec

  // types that combine tag + length (or other info) in a single byte
  TAG_AND_LEN = (byte) (1 << 5),
      STR = (byte) (2 << 5),
      SINT = (byte) (3 << 5),
      SLONG = (byte) (4 << 5),
      ARR = (byte) (5 << 5), //
      ORDERED_MAP = (byte) (6 << 5), // SimpleOrderedMap (a NamedList subclass, and more common)
      NAMED_LST = (byte) (7 << 5), // NamedList
      EXTERN_STRING = (byte) (8 << 5);


  private static final int MAX_UTF8_SIZE_FOR_ARRAY_GROW_STRATEGY = 65536;
  public static final int SZ = 1024 << 2;

  private static final byte VERSION = 2;
  private final ObjectResolver resolver;
  protected OutputStream daos;
  private StringCache stringCache;
  private WritableDocFields writableDocFields;
  private boolean alreadyMarshalled;
  private boolean alreadyUnmarshalled;
  protected AtomicBoolean readStringAsCharSeq = new AtomicBoolean();

  byte[] bytes;

  public JavaBinCodec() {
    resolver =null;
    writableDocFields =null;
  }

  public JavaBinCodec setReadStringAsCharSeq(boolean flag) {
    readStringAsCharSeq.set(flag);
    return this;
  }

  /**
   * Use this to use this as a PushWriter. ensure that close() is called explicitly after use
   *
   * @param os The output stream
   */
  public JavaBinCodec(FastOutputStream os, ObjectResolver resolver) throws IOException {
    this.resolver = resolver;
    initWrite(os);
  }

  public JavaBinCodec(ExpandableDirectBufferOutputStream os, ObjectResolver resolver) throws IOException {
    this.resolver = resolver;
    initWrite(os);
  }

  public JavaBinCodec(ObjectResolver resolver) {
    this(resolver, null);
  }
  public JavaBinCodec setWritableDocFields(WritableDocFields writableDocFields){
    this.writableDocFields = writableDocFields;
    return this;

  }

  public JavaBinCodec(ObjectResolver resolver, StringCache stringCache) {
    this.resolver = resolver;
    this.stringCache = stringCache;
  }

  public ObjectResolver getResolver() {
    return resolver;
  }

  public void marshal(Object nl, OutputStream os) throws IOException {
    try {
      initWrite(os);
      writeVal(nl);
    } finally {
      alreadyMarshalled = true;
      daos.flush();
      readStringAsCharSeq.set(false);
    }
  }

  public void marshal(Object nl, BufferedChannel os) throws IOException {
    try {
      initWrite(os);
      writeVal(nl);
    } finally {
      alreadyMarshalled = true;
      daos.flush();
      readStringAsCharSeq.set(false);
    }
  }

  public void marshal(Object nl, DirectBufferOutputStream os) throws IOException {
    try {
      initWrite(os);
      writeVal(nl);
    } finally {
      alreadyMarshalled = true;
      daos.flush();
      readStringAsCharSeq.set(false);
    }
  }

  public void marshal(Object nl, ExpandableDirectBufferOutputStream os) throws IOException {
    try {
      initWrite(os);
      writeVal(nl);
    } finally {
      alreadyMarshalled = true;
      daos.flush();
      readStringAsCharSeq.set(false);
    }
  }

  protected void initWrite(OutputStream os) throws IOException {
    assert !alreadyMarshalled;
    this.daos = os;
    write(VERSION);
  }

  protected void initWrite(DirectBufferOutputStream os) throws IOException {
    assert !alreadyMarshalled;
    this.daos = os;
    write(VERSION);
  }

  /** expert: sets a new output stream */
  public void init(OutputStream os) {
    daos = os;
  }

  public void init(BufferedChannel os) {
    daos = os;
  }

  byte version;

  public Object unmarshal(byte[] buf) throws IOException {
    FastInputStream dis = initRead(buf);
    return readVal(dis);
  }
  public Object unmarshal(InputStream is) throws IOException {
    DataInputInputStream dis = initRead(is);
    return readVal((InputStream) dis);
  }

  protected FastInputStream initRead(InputStream is) throws IOException {
    assert !alreadyUnmarshalled;
   // FastInputStream dis = new FastInputStream(is);
    if (!(is instanceof FastInputStream)) {
      is = new FastInputStream(is);
      return _init((FastInputStream) is);
    }
    return _init((FastInputStream) is);
  }
  protected FastInputStream initRead(byte[] buf) throws IOException {
    assert !alreadyUnmarshalled;

    UnsafeBuffer buffer = new UnsafeBuffer(buf);
    FastInputStream dis = new FastInputStream(new DirectBufferInputStream(buffer), 0);
    return _init(dis);
  }

  protected DataInputInputStream _init(DataInputStream dis) throws IOException {
    version = dis.readByte();
    if (version != VERSION) {
      throw new RuntimeException("Invalid version (expected " + VERSION +
          ", but " + version + ") or the data in not in 'javabin' format");
    }

    alreadyUnmarshalled = true;
    return new SolrInputStream(dis);
  }

  protected FastInputStream _init(FastInputStream dis) throws IOException {
    version = dis.readByte();
    if (version != VERSION) {
      throw new RuntimeException("Invalid version (expected " + VERSION +
          ", but " + version + ") or the data in not in 'javabin' format");
    }

    alreadyUnmarshalled = true;
    return dis;
  }


  public SimpleOrderedMap<Object> readOrderedMap(InputStream dis) throws IOException {
    log.info("read ordered map");
    int sz = readSize(dis);
    SimpleOrderedMap<Object> nl = new SimpleOrderedMap<>(sz);
    for (int i = 0; i < sz; i++) {

      String name = readVal(dis).toString();
      Object val = readVal(dis);
      nl.add(name, val);
    }
    return nl;
  }

  public NamedList<Object> readNamedList(InputStream dis) throws IOException {
    log.info("read namedlist");
    int sz = readSize(dis);
    NamedList<Object> nl = new NamedList<>(sz);
    for (int i = 0; i < sz; i++) {

      Object name = readVal(dis);
      if (name != null && !(name instanceof String)) {
        name = name.toString();
      }
      Object val = readVal(dis);
      nl.add((String) name, val);
    }
    return nl;
  }

  public void writeNamedList(NamedList<?> nl) throws IOException {
    log.info("write namedlist");
    int size = nl.size();
    writeTag(nl instanceof SimpleOrderedMap ? ORDERED_MAP : NAMED_LST, size);
    for (int i = 0; i < size; i++) {
      String name = nl.getName(i);
      writeExternString(name);
      Object val = nl.getVal(i);
      writeVal(val);
    }
  }

  public void writeVal(Object val) throws IOException {
    if (writeKnownType(val)) {
      return;
    } else {
      ObjectResolver resolver;
      if(val instanceof ObjectResolver) {
        resolver = (ObjectResolver)val;
      }
      else {
        resolver = this.resolver;
      }
      if (resolver != null) {
        Object tmpVal = resolver.resolve(val, this);
        if (tmpVal == null) return; // null means the resolver took care of it fully
        if (writeKnownType(tmpVal)) return;
      }
    }
    // Fallback to do *something*.
    // note: if the user of this codec doesn't want this (e.g. UpdateLog) it can supply an ObjectResolver that does
    //  something else like throw an exception.
    writeVal(val.getClass().getName() + ':' + val.toString());
  }

  protected static final Object END_OBJ = new Object();

  protected byte tagByte;

  public Object readVal(InputStream dis) throws IOException {
    tagByte = read(dis);
    return readObject(dis);
  }

  protected Object readObject(InputStream dis) throws IOException {
    log.info("read object tag={}", tagByte);

     if ((tagByte & 0xe0) == 0) {
       // if top 3 bits are clear, this is a normal tag
       log.info("norm tag");
       switch (tagByte) {
         case NULL:
           return null;
         case DATE:
           return new Date(readLong(dis));
         case INT:
           log.info("read INT TYPE");
           return readInt(dis);
         case BOOL_TRUE:
           return Boolean.TRUE;
         case BOOL_FALSE:
           return Boolean.FALSE;
         case FLOAT:
           return readFloat(dis);
         case DOUBLE:
           return readDouble(dis);
         case LONG:
           return readLong(dis);
         case BYTE:
           return read(dis);
         case SHORT:
           return readShort(dis);
         case MAP:
           return readMap(dis);
         case SOLRDOC:
           return readSolrDocument(dis);
         case SOLRDOCLST:
           return readSolrDocumentList(dis);
         case BYTEARR:
           return readByteArray(dis);
         case ITERATOR:
           return readIterator(dis);
         case END:
           return END_OBJ;
         case SOLRINPUTDOC:
           return readSolrInputDocument(dis);
         case ENUM_FIELD_VALUE:
           return readEnumFieldValue(dis);
         case MAP_ENTRY:
           return readMapEntry(dis);
         case MAP_ENTRY_ITER:
           return readMapIter(dis);
       }

     }

    log.info("tag with size");
    // OK, try type + size in single byte
    switch (tagByte >>> 5) {

      case STR >>> 5:
        return _readStr(dis, null, 0);
      case SINT >>> 5:
        return readSmallInt(dis);
      case SLONG >>> 5:
        return readSmallLong(dis);
      case ARR >>> 5:
        return readArray(dis);
      case ORDERED_MAP >>> 5:
        return readOrderedMap(dis);
      case NAMED_LST >>> 5:
        return readNamedList(dis);
      case EXTERN_STRING >>> 5:
        return readExternString(dis);
    }


    throw new RuntimeException("Unknown type " + tagByte);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public boolean writeKnownType(Object val) throws IOException {
    while (true) {
      if (writePrimitive(val)) return true;
      if (val instanceof NamedList) {
        writeNamedList((NamedList<?>) val);
        return true;
      }
      if (val instanceof SolrDocumentList) { // SolrDocumentList is a List, so must come before List check
        writeSolrDocumentList((SolrDocumentList) val);
        return true;
      }
      if (val instanceof SolrInputField) {
        val = ((SolrInputField) val).getValue();
        continue;
      }
      if (val instanceof IteratorWriter) {
        writeIterator((IteratorWriter) val);
        return true;
      }
      if (val instanceof Collection) {
        writeArray((Collection) val);
        return true;
      }
      if (val instanceof Object[]) {
        writeArray((Object[]) val);
        return true;
      }
      if (val instanceof SolrDocument) {
        //this needs special treatment to know which fields are to be written
        writeSolrDocument((SolrDocument) val);
        return true;
      }
      if (val instanceof SolrInputDocument) {
        writeSolrInputDocument((SolrInputDocument) val);
        return true;
      }
      if (val instanceof MapWriter) {
        writeMap((MapWriter) val);
        return true;
      }
      if (val instanceof Map) {
        writeMap((Map) val);
        return true;
      }
      if (val instanceof Iterator) {
        writeIterator((Iterator) val);
        return true;
      }
      if (val instanceof Path) {
        writeStr(((Path) val).toAbsolutePath().toString());
        return true;
      }
      if (val instanceof Iterable) {
        writeIterator(((Iterable) val).iterator());
        return true;
      }
      if (val instanceof EnumFieldValue) {
        writeEnumFieldValue((EnumFieldValue) val);
        return true;
      }
      if (val instanceof Map.Entry) {
        writeMapEntry((Entry) val);
        return true;
      }
      if (val instanceof MapSerializable) {
        //todo find a better way to reuse the map more efficiently
        writeMap(((MapSerializable) val).toMap(new NamedList().asShallowMap()));
        return true;
      }
      if (val instanceof AtomicInteger) {
        writeInt(((AtomicInteger) val).get());
        return true;
      }
      if (val instanceof AtomicLong) {
        writeLong(((AtomicLong) val).get());
        return true;
      }
      if (val instanceof AtomicBoolean) {
        writeBoolean(((AtomicBoolean) val).get());
        return true;
      }
      return false;
    }
  }

  private static class MyEntry implements Entry<Object,Object> {

    private final Object key;
    private final Object value;

    public MyEntry(Object key, Object value) {
      this.key = key;
      this.value = value;
    }

    @Override
    public Object getKey() {
      return key;
    }

    @Override
    public Object getValue() {
      return value;
    }

    @Override
    public String toString() {
      return "MapEntry[" + key + ":" + value + "]";
    }

    @Override
    public Object setValue(Object value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int hashCode() {
      int result = 31;
      result *=31 + key.hashCode();
      result *=31 + value.hashCode();
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if(this == obj) {
        return true;
      }
      if (obj instanceof Map.Entry<?, ?>) {
        Entry<?, ?> entry = (Entry<?, ?>) obj;
        return (this.key.equals(entry.getKey()) && this.value.equals(entry.getValue()));
      }
      return false;
    }
  }

  public static class BinEntryWriter implements MapWriter.EntryWriter {
    private final JavaBinCodec codec;

    BinEntryWriter(JavaBinCodec codec) {
      this.codec = codec;
    }


    @Override
    public MapWriter.EntryWriter put(CharSequence k, Object v) throws IOException {
      codec.writeExternString(k);
      codec.writeVal(v);
      return this;
    }

    @Override
    public MapWriter.EntryWriter put(CharSequence k, int v) throws IOException {
      codec.writeExternString(k);
      codec.writeInt(v);
      return this;
    }

    @Override
    public MapWriter.EntryWriter put(CharSequence k, long v) throws IOException {
      codec.writeExternString(k);
      codec.writeLong(v);
      return this;
    }

    @Override
    public MapWriter.EntryWriter put(CharSequence k, float v) throws IOException {
      codec.writeExternString(k);
      codec.writeFloat(v);
      return this;
    }

    @Override
    public MapWriter.EntryWriter put(CharSequence k, double v) throws IOException {
      codec.writeExternString(k);
      codec.writeDouble(v);
      return this;
    }

    @Override
    public MapWriter.EntryWriter put(CharSequence k, boolean v) throws IOException {
      codec.writeExternString(k);
      codec.writeBoolean(v);
      return this;
    }

    @Override
    public MapWriter.EntryWriter put(CharSequence k, CharSequence v) throws IOException {
      codec.writeExternString(k);
      codec.writeStr(v);
      return this;
    }

    private BiConsumer<CharSequence, Object> biConsumer;

    @Override
    public BiConsumer<CharSequence, Object> getBiConsumer() {
      if (biConsumer == null) biConsumer = MapWriter.EntryWriter.super.getBiConsumer();
      return biConsumer;
    }
  }

  public final BinEntryWriter ew = new BinEntryWriter(this);


  public void writeMap(MapWriter val) throws IOException {
    log.info("write map");
    writeTag(MAP_ENTRY_ITER);
    val.writeMap(ew);
    writeTag(END);
  }


  public void writeTag(byte tag) throws IOException {
    log.info("write tag {}", tag);
    write(tag);
  }

  public void writeTag(byte tag, int size) throws IOException {
    log.info("write tag {} sz={}", tag, size);
    if ((tag & 0xe0) != 0) {
      if (size < 0x1f) {
        write((tag | size));
      } else {
        write((tag | 0x1f));
        writeVInt(size - 0x1f, daos);
      }
    } else {
      write(tag);
      writeVInt(size, daos);
    }
  }

  private void write(int tag) throws IOException {
    daos.write((byte)tag);
  }

  public void writeByteArray(byte[] arr, int offset, int len) throws IOException {
    log.info("write byte array");
    writeTag(BYTEARR, len);
    daos.write(arr, offset, len);
  }

  public byte[] readByteArray(InputStream dis) throws IOException {
    byte[] arr = new byte[readVInt(dis)];
    readFully(dis, arr);
    return arr;
  }

  public void readFully(InputStream dis, byte b[]) throws IOException {
    readFully(dis, b, 0, b.length);
  }

  public static void readFully(InputStream dis, byte b[], int off, int len) throws IOException {
    while (len>0) {
      int ret = dis.read(b, off, len);
      if (ret==-1) {
        throw new EOFException();
      }
      off += ret;
      len -= ret;
    }
  }

  //use this to ignore the writable interface because , child docs will ignore the fl flag
  // is it a good design?
  private boolean ignoreWritable =false;
  private MapWriter.EntryWriter cew;

  public void writeSolrDocument(SolrDocument doc) throws IOException {
    log.info("write solrdoc");
    List<SolrDocument> children = doc.getChildDocuments();
    int fieldsCount = 0;
    if(writableDocFields == null || writableDocFields.wantsAllFields() || ignoreWritable){
      fieldsCount = doc.size();
    } else {
      for (Entry<String, Object> e : doc) {
        if(toWrite(e.getKey())) fieldsCount++;
      }
    }
    int sz = fieldsCount + (children==null ? 0 : children.size());
    writeTag(SOLRDOC);
    writeTag(ORDERED_MAP, sz);
    if (cew == null) cew = new ConditionalKeyMapWriter.EntryWriterWrapper(ew, (k) -> toWrite(k.toString()));
    doc.writeMap(cew);
    if (children != null) {
      try {
        ignoreWritable = true;
        for (SolrDocument child : children) {
          writeSolrDocument(child);
        }
      } finally {
        ignoreWritable = false;
      }
    }

  }

  protected boolean toWrite(String key) {
    return writableDocFields == null || ignoreWritable || writableDocFields.isWritable(key);
  }

  public SolrDocument readSolrDocument(InputStream dis) throws IOException {
    log.info("read solrdoc");
    tagByte = read(dis);
    int size = readSize(dis);
    SolrDocument doc = new SolrDocument(new LinkedHashMap<>(size));
    for (int i = 0; i < size; i++) {
      String fieldName;
      Object obj = readVal(dis); // could be a field name, or a child document
      if (obj instanceof SolrDocument) {
        doc.addChildDocument((SolrDocument)obj);
        continue;
      } else {
        fieldName = (String)obj;
      }
      Object fieldVal = readVal(dis);
      doc.setField(fieldName, fieldVal);
    }
    return doc;
  }

  public SolrDocumentList readSolrDocumentList(InputStream dis) throws IOException {
    log.info("read solrdoc list");
    SolrDocumentList solrDocs = new SolrDocumentList();
    @SuppressWarnings("unchecked")
    List<Object> list = (List<Object>) readVal(dis);
    solrDocs.setNumFound((Long) list.get(0));
    solrDocs.setStart((Long) list.get(1));
    solrDocs.setMaxScore((Float) list.get(2));
    if (list.size() > 3) { //needed for back compatibility
      solrDocs.setNumFoundExact((Boolean)list.get(3));
    }

    @SuppressWarnings("unchecked")
    List<SolrDocument> l = (List<SolrDocument>) readVal(dis);
    solrDocs.addAll(l);
    return solrDocs;
  }

  public void writeSolrDocumentList(SolrDocumentList docs)
      throws IOException {
    log.info("write solrdoclist");
    writeTag(SOLRDOCLST);
    List<Object> l = new ArrayList<>(4);
    l.add(docs.getNumFound());
    l.add(docs.getStart());
    l.add(docs.getMaxScore());
    l.add(docs.getNumFoundExact());
    writeArray(l);
    writeArray(docs);
  }

  public SolrInputDocument readSolrInputDocument(InputStream dis) throws IOException {
    log.info("read solrdoc in");
    int sz = readVInt(dis);
//    float docBoost = (Float)readVal(dis);
//    if (docBoost != 1f) {
//      String message = "Ignoring document boost: " + docBoost + " as index-time boosts are not supported anymore";
//      if (WARNED_ABOUT_INDEX_TIME_BOOSTS.compareAndSet(false, true)) {
//        log.warn(message);
//      } else {
//        log.debug(message);
//      }
//    }
    SolrInputDocument sdoc = createSolrInputDocument(sz);
    for (int i = 0; i < sz; i++) {
      String fieldName;
      Object obj = readVal(dis); // could be a boost, a field name, or a child document
      if (obj instanceof SolrInputDocument) {
        sdoc.addChildDocument((SolrInputDocument)obj);
        continue;
      } else {
        fieldName = (String)obj;
      }
      Object fieldVal = readVal(dis);
      sdoc.setField(fieldName, fieldVal);
    }
    return sdoc;
  }

  protected SolrInputDocument createSolrInputDocument(int sz) {
    return new SolrInputDocument(new LinkedHashMap<>(sz));
  }
  static final Predicate<CharSequence> IGNORECHILDDOCS = it -> !CommonParams.CHILDDOC.equals(it.toString());

  public void writeSolrInputDocument(SolrInputDocument sdoc) throws IOException {
    List<SolrInputDocument> children = sdoc.getChildDocuments();
    int sz = sdoc.size() + (children==null ? 0 : children.size());
    writeTag(SOLRINPUTDOC, sz);
    sdoc.writeMap(new ConditionalKeyMapWriter.EntryWriterWrapper(ew,IGNORECHILDDOCS));
    if (children != null) {
      for (SolrInputDocument child : children) {
        writeSolrInputDocument(child);
      }
    }
  }


  public Map<Object, Object> readMapIter(InputStream dis) throws IOException {
    Map<Object, Object> m = newMap(-1);
    for (; ; ) {
      Object key = readVal(dis);
      if (key == END_OBJ) break;
      Object val = readVal(dis);
      m.put(key, val);
    }
    return m;
  }

  /**
   * create a new Map object
   * @param size expected size, -1 means unknown size
   */
  protected Map<Object, Object> newMap(int size) {
    return size < 0 ? new LinkedHashMap<>() : new LinkedHashMap<>(size);
  }

  public Map<Object,Object> readMap(InputStream dis)
      throws IOException {
    log.info("read map");
    int sz = readVInt(dis);
    return readMap(dis, sz);
  }

  protected Map<Object, Object> readMap(InputStream dis, int sz) throws IOException {
    Map<Object, Object> m = newMap(sz);
    for (int i = 0; i < sz; i++) {
      Object key = readVal(dis);
      Object val = readVal(dis);
      m.put(key, val);

    }
    return m;
  }

  public final ItemWriter itemWriter = new ItemWriter() {
    @Override
    public ItemWriter add(Object o) throws IOException {
      writeVal(o);
      return this;
    }

    @Override
    public ItemWriter add(int v) throws IOException {
      writeInt(v);
      return this;
    }

    @Override
    public ItemWriter add(long v) throws IOException {
      writeLong(v);
      return this;
    }

    @Override
    public ItemWriter add(float v) throws IOException {
      writeFloat(v);
      return this;
    }

    @Override
    public ItemWriter add(double v) throws IOException {
      writeDouble(v);
      return this;
    }

    @Override
    public ItemWriter add(boolean v) throws IOException {
      writeBoolean(v);
      return this;
    }
  };

  @Override
  public void writeIterator(IteratorWriter val) throws IOException {
    log.info("write itr");
    writeTag(ITERATOR);
    val.writeIter(itemWriter);
    writeTag(END);
  }
  public void writeIterator(@SuppressWarnings({"rawtypes"})Iterator iter) throws IOException {
    log.info("write itr");
    writeTag(ITERATOR);
    while (iter.hasNext()) {
      writeVal(iter.next());
    }
    writeTag(END);
  }

  public List<Object> readIterator(InputStream fis) throws IOException {
    ArrayList<Object> l = new ArrayList<>();
    while (true) {
      Object o = readVal(fis);
      if (o == END_OBJ) break;
      l.add(o);
    }
    return l;
  }

  public void writeArray(@SuppressWarnings({"rawtypes"})List l) throws IOException {
    log.info("write array list");
    final int size = l.size();
    writeTag(ARR, size);
    for (Object o : l) {
      writeVal(o);
    }
  }

  public void writeArray(@SuppressWarnings({"rawtypes"})Collection coll) throws IOException {
    log.info("write array coll");
    writeTag(ARR, coll.size());
    for (Object o : coll) {
      writeVal(o);
    }

  }

  public void writeArray(Object[] arr) throws IOException {
    log.info("write array obj");
    int sz = arr.length;
    writeTag(ARR, sz);
    for (Object o : arr) {
      writeVal(o);
    }
  }

  @SuppressWarnings({"unchecked"})
  public List<Object> readArray(InputStream dis) throws IOException {

    int sz = readSize(dis);
    log.info("read array sz, {}", sz );
    return readArray(dis, sz);
  }

  @SuppressWarnings({"rawtypes"})
  protected List readArray(InputStream dis, int sz) throws IOException {
    log.info("read array of size {}", sz);
    ArrayList<Object> l = new ArrayList<>(sz);
    for (int i = 0; i < sz; i++) {
      l.add(readVal(dis));
    }
    log.info("return list {}", l);
    return l;
  }

  /**
   * write {@link EnumFieldValue} as tag+int value+string value
   * @param enumFieldValue to write
   */
  public void writeEnumFieldValue(EnumFieldValue enumFieldValue) throws IOException {
    log.info("write enum");
    writeTag(ENUM_FIELD_VALUE);
    writeInt(enumFieldValue.toInt());
    writeStr(enumFieldValue.toString());
  }

  public void writeMapEntry(Map.Entry<?,?> val) throws IOException {
    log.info("write map entry");
    writeTag(MAP_ENTRY);
    writeVal(val.getKey());
    writeVal(val.getValue());
  }

  /**
   * read {@link EnumFieldValue} (int+string) from input stream
   * @param dis data input stream
   * @return {@link EnumFieldValue}
   */
  public EnumFieldValue readEnumFieldValue(InputStream dis) throws IOException {
    log.info("read enum");
    Integer intValue = (Integer) readVal(dis);
    String stringValue = (String) readVal(dis);
    return new EnumFieldValue(intValue, stringValue);
  }


  public Map.Entry<Object,Object> readMapEntry(InputStream dis) throws IOException {
    log.info("read mapentry");
    final Object key = readVal(dis);
    final Object value = readVal(dis);
    return new MyEntry(key, value);
  }

  /**
   * write the string as tag+length, with length being the number of UTF-8 bytes
   */
  public void writeStr(CharSequence s) throws IOException {
    log.info("write str");
    if (s == null) {
      writeTag(NULL);
      return;
    }

    ////      writeTag(X_STRING, ((String) s).getBytes(UTF_8).length);
    ////      MutableDirectBuffer buffer = ((ExpandableDirectBufferOutputStream) daos).buffer();
    ////
    ////      int written = buffer.putStringWithoutLengthUtf8(((ExpandableDirectBufferOutputStream) daos).position(), (String) s);
    ////      ((ExpandableDirectBufferOutputStream) daos).setPosition(((ExpandableDirectBufferOutputStream) daos).position() + written);
    ////
  //  if ( s instanceof String && (daos instanceof ExpandableDirectBufferOutputStream || daos instanceof BufferedChannel)) {

      byte[] bytes = ((String) s).getBytes(UTF_8);
    writeTag(STR, bytes.length);

   // if (writeSize)   writeVInt(bytes.length, daos);

      daos.write(bytes, 0, bytes.length);
      return;
//    } else {
//        throw new IllegalStateException(s.getClass().getName() + " " + daos.getClass().getName());
//    }




//    if (s instanceof Utf8CharSequence) {
//      writeUTF8Str((Utf8CharSequence) s);
//      return;
//    }



//    int end = s.length();
//    int maxSize = end * ByteUtils.MAX_UTF8_BYTES_PER_CHAR;
//
//    if (maxSize <= MAX_UTF8_SIZE_FOR_ARRAY_GROW_STRATEGY) {
//      MutableDirectBuffer brr = ExpandableBuffers.getInstance().acquire(-1, true);
//      try {
//        int sz = ByteUtils.UTF16toUTF8(s, 0, end, brr, 0);
//
//        brr.byteBuffer().limit(brr.byteBuffer().position());
//        brr.byteBuffer().position(0);
//        writeTag(STR, sz);
//
//        int index = brr.byteBuffer().position();
//        int cnt = 0;
//        while (cnt < sz) {
//          daos.write(brr.getByte(index++));
//          cnt++;
//        }
//      } finally {
//        ExpandableBuffers.getInstance().release(brr);
//      }
//    } else {
//      // double pass logic for large strings, see SOLR-7971
//      int sz = ByteUtils.calcUTF16toUTF8Length(s, 0, end);
//      writeTag(STR, sz);
//      MutableDirectBuffer brr = ExpandableBuffers.getInstance().acquire(-1, true);
//      try {
//        ByteUtils.writeUTF16toUTF8(s, 0, end, daos, brr);
//      } finally {
//        ExpandableBuffers.getInstance().release(brr);
//      }
//
//    }
  }


  public String readStr(InputStream dis, StringCache stringCache) throws IOException {
    return _readStr(dis, stringCache, null);
  }


  private String _readStr(InputStream dis, StringCache stringCache, Integer size) throws IOException {
//    int sz;
//    if (size == null) {
//      sz = readVInt(dis);
//    } else {
//      sz = size;
//    }
    int sz = readSize(dis);
    byte[] bytes = new byte[sz];
    readFully(dis, bytes);
    return new String(bytes, UTF_8);

//    MutableDirectBuffer brr = ExpandableBuffers.getInstance().acquire(-1, true);
//    try {
//      int sz;
//      if (size != null)  {
//        sz = size;
//      } else {
//        sz = readVInt(dis);
//      }
//
//      for (int i = 0; i < sz; i++) {
//
//        brr.putByte(i, dis.readByte());
//      }
//
//      brr.byteBuffer().limit(brr.byteBuffer().position());
//      brr.byteBuffer().position(0);
//      //
//      //
//      //////        if (stringCache != null) {
//      //////          return stringCache.get(bytesRef.reset(b, 0, sz));
//      //////        } else {
//      //  CharArr arr = getCharArr(sz);
//      //  ByteUtils.UTF8toUTF16(brr, 0, sz, arr);
//
//      return brr.getStringWithoutLengthUtf8(0, sz);
//    } finally {
//      ExpandableBuffers.getInstance().release(brr);
//    }
  }
  //  }

  /////////// code to optimize reading UTF8


  protected CharSequence readUtf8(InputStream dis) throws IOException {

    return _readStr(dis, null, null);
  }
//
//  protected CharSequence readUtf8(DataInputInputStream dis, int sz) throws IOException {
//    ByteArrayUtf8CharSequence result = new ByteArrayUtf8CharSequence(null,0,0);
////    if(dis.readDirectUtf8(result, sz)){
////      result.stringProvider= getStringProvider();
////      return result;
////    }
////
////    if (sz > MAX_UTF8_SZ) return _readStr(dis, null, sz);
//    if (bytesBlock == null) bytesBlock = new BytesBlock(SZ);
//    BytesBlock block = this.bytesBlock.expand(sz);
//    dis.readFully(block.getBuf(), block.getStartPos(), sz);
//
//    result.reset(block.getBuf(), block.getStartPos(), sz,null);
//    result.stringProvider = getStringProvider();
//    return result;
//  }

//  private Function<ByteArrayUtf8CharSequence, String> getStringProvider() {
//    if (stringProvider == null) {
//      stringProvider = new ByteArrayUtf8CharSequenceStringFunction();
//    }
//    return this.stringProvider;
//  }

  public void writeInt(int v) throws IOException {

    if (v > 0) {
      int b = SINT | (v & 0x0f);

      if (v >= 0x0f) {
        b |= 0x10;
        write(b);
        writeVInt(v >>> 4, daos);
        log.info("writeint var {}", b);
      } else {
        log.info("writeint single byte {}", b);
        write(b);
      }

    } else {
      log.info("writeint data out");
      write(INT);
      daos.write((v >>> 24) & 0xFF);
      daos.write((v >>> 16) & 0xFF);
      daos.write((v >>>  8) & 0xFF);
      daos.write((v >>>  0) & 0xFF);
    }
  }

  public int readSmallInt(InputStream dis) throws IOException {
    log.info("read small int");
    int v = tagByte & 0x0F;
    if ((tagByte & 0x10) != 0)
      v = (readVInt(dis) << 4) | v;
    return v;
  }

  public void writeLong(long val) throws IOException {

    log.info("write long");
    if ((val & 0xff00000000000000L) == 0) {
      int b = SLONG | ((int) val & 0x0f);
      if (val >= 0x0f) {
        b |= 0x10;
        write(b);
        writeVLong(val >>> 4, daos);
      } else {
        write(b);
      }
    } else {
      write(LONG);
      writeLongToOut(val);
    }
  }

  public long readSmallLong(InputStream dis) throws IOException {
    log.info("read small long");
    long v = tagByte & 0x0F;
    if ((tagByte & 0x10) != 0)
      v = (readVLong(dis) << 4) | v;
    return v;
  }

  public void writeFloat(float val) throws IOException {
    log.info("write float");
    write(FLOAT);
    writeFloatToOut(val);
  }

  public boolean writePrimitive(Object val) throws IOException {
    if (val == null) {
      write(NULL);
      return true;
    } else if (val instanceof Utf8CharSequence) {
      writeUTF8Str((Utf8CharSequence) val);
      return true;
    } else if (val instanceof CharSequence) {
      writeStr((CharSequence) val);
      return true;
    } else if (val instanceof Number) {

      if (val instanceof Integer) {
        log.info("write prim INT TYPE");
        writeInt((Integer) val);
        return true;
      } else if (val instanceof Long) {
        writeLong((Long) val);
        return true;
      } else if (val instanceof Float) {
        writeFloat((Float) val);
        return true;
      } else if (val instanceof Double) {
        writeDouble((Double) val);
        return true;
      } else if (val instanceof Byte) {
        log.info("write BYTE");
        write(BYTE);
        write(((Byte) val).intValue());
        return true;
      } else if (val instanceof Short) {
        write(SHORT);
        writeShortToOut(((Short) val).intValue());
        return true;
      }
      return false;

    } else if (val instanceof Date) {
      write(DATE);
      writeLongToOut(((Date) val).getTime());
      return true;
    } else if (val instanceof Boolean) {
      writeBoolean((Boolean) val);
      return true;
    } else if (val instanceof byte[]) {
      writeByteArray((byte[]) val, 0, ((byte[]) val).length);
      return true;
    } else if (val instanceof ByteBuffer) {
      ByteBuffer buf = (ByteBuffer) val;
      writeByteArray(buf.array(),buf.position(),buf.limit() - buf.position());
      return true;
    } else if (val == END_OBJ) {
      writeTag(END);
      return true;
    }
    return false;
  }

  protected void writeBoolean(boolean val) throws IOException {
    log.info("write bool");
    if (val) write(BOOL_TRUE);
    else write(BOOL_FALSE);
  }

  protected void writeDouble(double val) throws IOException {
    log.info("write double");
    write(DOUBLE);
    writeDoubleToOut(val);
  }


  public void writeMap(Map<?,?> val) throws IOException {
    log.info("write map");
    writeTag(MAP, val.size());
    if (val instanceof MapWriter) {
      ((MapWriter) val).writeMap(ew);
      return;
    }
    for (Map.Entry<?,?> entry : val.entrySet()) {
      Object key = entry.getKey();
      if (key instanceof String) {
        writeExternString((String) key);
      } else {
        writeVal(key);
      }
      writeVal(entry.getValue());
    }
  }


  public int readSize(InputStream in) throws IOException {
    int sz = tagByte & 0x1f;
    if (sz == 0x1f) sz += readVInt(in);
    log.info("read sz {}", sz);
    return sz;
  }


  /**
   * Special method for variable length int (copied from lucene). Usually used for writing the length of a
   * collection/array/map In most of the cases the length can be represented in one byte (length &lt; 127) so it saves 3
   * bytes/object
   *
   * @throws IOException If there is a low-level I/O error.
   */
  public static void writeVInt(int i, OutputStream out) throws IOException {
    log.info("write vint");
    while ((i & ~0x7F) != 0) {
      out.write((byte) ((i & 0x7f) | 0x80));
      i >>>= 7;
    }
    out.write((byte) i);
  }

  /**
   * The counterpart for {@link #writeVInt(int, OutputStream)}
   *
   * @throws IOException If there is a low-level I/O error.
   */
  public static int readVInt(InputStream in) throws IOException {
    byte b = read(in);
    int i = b & 0x7F;
    for (int shift = 7; (b & 0x80) != 0; shift += 7) {
      b = read(in);
      i |= (b & 0x7F) << shift;
    }
    return i;
  }

  protected static byte read(InputStream in) throws IOException {
    int r = in.read();
//    if (r == -1) {
//      throw new EOFException();
//    }
    return (byte) r;
  }


  public void writeVLong(long i, OutputStream out) throws IOException {
    log.info("write vlong");
    while ((i & ~0x7F) != 0) {
      write((byte) ((i & 0x7f) | 0x80));
      i >>>= 7;
    }
    write((byte) i);
  }

  public int readInt(InputStream in) throws IOException {

    int v =  ((in.read() << 24)
            |(in.read() << 16)
            |(in.read() << 8)
            | in.read());
    log.info("read int {}", v);
    return v;
  }

  public long readLong(InputStream in) throws IOException {
    log.info("read long");
    return  (((long)readUnsignedByte(in)) << 56)
            | (((long)readUnsignedByte(in)) << 48)
            | (((long)readUnsignedByte(in)) << 40)
            | (((long)readUnsignedByte(in)) << 32)
            | (((long)readUnsignedByte(in)) << 24)
            | (readUnsignedByte(in) << 16)
            | (readUnsignedByte(in) << 8)
            | (readUnsignedByte(in));
  }

  public float readFloat(InputStream is) throws IOException {
    return Float.intBitsToFloat(readInt(is));
  }

  public double readDouble(InputStream is) throws IOException {
    log.info("read double");
    return Double.longBitsToDouble(readLong(is));
  }

  public short readShort(InputStream in) throws IOException {
    log.info("read short");
    return (short)((readUnsignedByte(in) << 8) | readUnsignedByte(in));
  }

  public int readUnsignedShort(InputStream in) throws IOException {
    return (readUnsignedByte(in) << 8) | readUnsignedByte(in);
  }

  public int readUnsignedByte(InputStream in) throws IOException {
    return in.read() & 0xff;
  }

  public static long readVLong(InputStream in) throws IOException {
    log.info("read vlong");
    byte b = read(in);
    long i = b & 0x7F;
    for (int shift = 7; (b & 0x80) != 0; shift += 7) {
      b = read(in);
      i |= (long) (b & 0x7F) << shift;
    }
    return i;
  }

  private int stringsCount = 0;
  private Map<String, Integer> stringsMap;
  private Map<Integer, String> indexMap;

  public void writeExternString(CharSequence s) throws IOException {

    if (s == null) {
      writeTag(NULL);
      return;
    }
    Integer idx = stringsMap == null ? null : stringsMap.get(s.toString());
    if (idx == null) idx = 0;

    writeTag(EXTERN_STRING, idx);

    if (idx == 0) {
      s = s.toString();
      writeStr(s);
      if (s.length() > 5) {
        if (stringsMap == null) stringsMap = new Object2IntOpenHashMap<>(16, 0.5f);
        stringsMap.put((String) s, ++stringsCount);
      }

    }
    log.info("write ext str idx=" + idx + " cnt=" + stringsCount);
  }

  public CharSequence readExternString(InputStream fis) throws IOException {
    log.info("read extern string");
    int idx = readSize(fis);
    if (idx != 0 && indexMap != null) {// idx != 0 is the index of the extern string

      String interned = indexMap.get(idx);
      if (interned != null) {
        return interned;
      }
    }
    // idx == 0 means it has a string value

      String s = (String) readVal(fis);

      if (indexMap == null) indexMap = new Int2ObjectOpenHashMap<>(16, 0.5f);
      indexMap.put(idx, s);
      return s;

  }


  public void writeUTF8Str(Utf8CharSequence utf8) throws IOException {
    log.info("write utf8str");


    byte[] buf = ((ByteArrayUtf8CharSequence) utf8).getBuf();
    int offset = ((ByteArrayUtf8CharSequence) utf8).offset();
    int sz = utf8.size();
    writeTag(STR, sz);


    daos.write(buf, offset, sz);

    return;

    //  writeTag(STR, utf8.size());
    // writeUtf8CharSeq(utf8);
  }

  public void writeShortToOut(int v) throws IOException {
    log.info("write short to out");
    write((byte)(v >>> 8) & 0xFF);
    write((byte)(v >>> 0) & 0xFF);
  }


  public void writeChar(int v) throws IOException {
    writeShortToOut(v);
  }

  public void writeLongToOut(long v) throws IOException {
    log.info("write long to out");
    write((byte)(v >>> 56));
    write((byte)(v >>> 48));
    write((byte)(v >>> 40));
    write((byte)(v >>> 32));
    write((byte)(v >>> 24));
    write((byte)(v >>> 16));
    write((byte)(v >>> 8));
    write((byte)(v));
  }

  public void writeIntToOut(int v) throws IOException {
    log.info("write int to out");
    write((byte)(v >>> 24) & 0xFF);
    write((byte)(v >>> 16) & 0xFF);
    write((byte)(v >>>  8) & 0xFF);
    write((byte)(v) & 0xFF);
  }

  public void writeFloatToOut(float v) throws IOException {
    log.info("write float to out");
    writeIntToOut(Float.floatToRawIntBits(v));
  }


  public void writeDoubleToOut(double v) throws IOException {
    log.info("write double to out");
    writeLongToOut(Double.doubleToRawLongBits(v));
  }

  /**
   * Allows extension of {@link JavaBinCodec} to support serialization of arbitrary data types.
   * <p>
   * Implementors of this interface write a method to serialize a given object using an existing {@link JavaBinCodec}
   */
  public interface ObjectResolver {
    /**
     * Examine and attempt to serialize the given object, using a {@link JavaBinCodec} to write it to a stream.
     *
     * @param o     the object that the caller wants serialized.
     * @param codec used to actually serialize {@code o}.
     * @return the object {@code o} itself if it could not be serialized, or {@code null} if the whole object was successfully serialized.
     * @see JavaBinCodec
     */
    Object resolve(Object o, JavaBinCodec codec) throws IOException;
  }

  public interface WritableDocFields {
    boolean isWritable(String name);
    boolean wantsAllFields();
  }


  public static class StringCache {
    private final Cache<StringBytes, String> cache;

    public StringCache(Cache<StringBytes, String> cache) {
      this.cache = cache;
    }

    public String get(StringBytes b) {
      String result = cache.get(b);
      if (result == null) {
        //make a copy because the buffer received may be changed later by the caller
        StringBytes copy = new StringBytes(Arrays.copyOfRange(b.bytes, b.offset, b.offset + b.length), 0, b.length);
        CharArr arr = new CharArr();
        ByteUtils.UTF8toUTF16(b.bytes, b.offset, b.length, arr);
        result = arr.toString();
        cache.put(copy, result);
      }
      return result;
    }
  }

  @Override
  public void close() throws IOException {
    if (daos != null) {
     daos.flush();
    }
  }

//  private static class ByteArrayUtf8CharSequenceStringFunction implements Function<ByteArrayUtf8CharSequence, String> {
//    final CharArr charArr = new CharArr(256);
//
//    @Override
//    public String apply(ByteArrayUtf8CharSequence butf8cs) {
//
//      charArr.reset();
//      ByteUtils.UTF8toUTF16(butf8cs.buf, butf8cs.offset(), butf8cs.size(), charArr);
//      return charArr.toString();
//    }
//  }
}
