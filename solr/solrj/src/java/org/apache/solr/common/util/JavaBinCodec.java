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

import static java.lang.Character.MAX_SURROGATE;
import static java.lang.Character.MIN_SURROGATE;
import static org.apache.solr.common.util.ByteArrayUtf8CharSequence.convertCharSeq;

import com.google.errorprone.annotations.DoNotCall;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.nio.charset.CharsetEncoder;
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
public class JavaBinCodec implements PushWriter {

  public static final String IGNORING_FIELD_BOOST_AS_INDEX_TIME_BOOSTS_ARE_NOT_SUPPORTED_ANYMORE_BOOST = "Ignoring field boost as index-time boosts are not supported anymore, boost=";
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final AtomicBoolean WARNED_ABOUT_INDEX_TIME_BOOSTS = new AtomicBoolean();

  private static final CharsetEncoder ENCODER = StandardCharsets.UTF_8.newEncoder();

  public static final byte NULL = 0,
      BOOL_TRUE = 1,
      BOOL_FALSE = 2,
      BYTE = 3,
      SHORT = 4,
      DOUBLE = 5,
      INT = 6,
      LONG = 7,
      FLOAT = 8,
      DATE = 9,
      MAP = 10,
      SOLRDOC = 11,
      SOLRDOCLST = 12,
      BYTEARR = 13,
      ITERATOR = 14,
      /** this is a special tag signals an end. No value is associated with it */
      END = 15,
      SOLRINPUTDOC = 16,
      MAP_ENTRY_ITER = 17,
      ENUM_FIELD_VALUE = 18,
      MAP_ENTRY = 19,
      UUID = 20, // This is reserved to be used only in LogCodec
      // types that combine tag + length (or other info) in a single byte
      TAG_AND_LEN = (byte) (1 << 5),
      STR = (byte) (1 << 5),
      SINT = (byte) (2 << 5),
      SLONG = (byte) (3 << 5),
      ARR = (byte) (4 << 5), //
      ORDERED_MAP = (byte) (5 << 5), // SimpleOrderedMap (a NamedList subclass, and more common)
      NAMED_LST = (byte) (6 << 5), // NamedList
      EXTERN_STRING = (byte) (7 << 5);

  // have not yet found where this is a win
  public static final int STRING_UTF_OVER_SOLR_UTIL_SZ_CUTOFF = 0;
  public static final int MAX_EXTERN_STRING_LENGTH = 40;

  public static final int MAX_EXTERN_STRING_CACHE_SZ = 128;

  public static final String UTF_8 = "UTF-8";
  public static final Float FLOAT_1 = 1f;

  private static final byte VERSION = 2;
  public static final Integer ZERO = 0;
  private final ObjectResolver resolver;
  private final boolean externStringLimits;
  protected JavaBinOutputStream daos;
  private StringCache stringCache;
  private WritableDocFields writableDocFields;
  private boolean alreadyMarshalled;
  private boolean alreadyUnmarshalled;
  protected boolean readStringAsCharSeq = false;

  private final int useStringUtf8Over;

  public JavaBinCodec() {
    resolver =null;
    writableDocFields =null;
    this.externStringLimits = true;
    useStringUtf8Over =
        Integer.getInteger("useStringUtf8Over", STRING_UTF_OVER_SOLR_UTIL_SZ_CUTOFF);
  }

  public JavaBinCodec setReadStringAsCharSeq(boolean flag) {
    readStringAsCharSeq = flag;
    return this;
  }

  /**
   * Instantiates a new Java bin codec to be used as a PushWriter.
   *
   * Ensure that close() is called explicitly after use.
   *
   * @param os               the OutputStream to marshal to
   * @param resolver         a resolver to be used for resolving Objects
   */
  public JavaBinCodec(OutputStream os, ObjectResolver resolver) {
    this.resolver = resolver;
    this.externStringLimits = true;
    useStringUtf8Over =
        Integer.getInteger("useStringUtf8Over", STRING_UTF_OVER_SOLR_UTIL_SZ_CUTOFF);
    initWrite(os, false);
  }

  /**
   * Instantiates a new Java bin codec to be used as a PushWriter.
   *
   * <p>Ensure that close() is called explicitly after use.
   *
   * @param os the OutputStream to marshal to
   * @param resolver a resolver to be used for resolving Objects
   * @param streamIsBuffered if true, no additional buffering for the OutputStream will be
   *     considered necessary.
   */
  public JavaBinCodec(
      OutputStream os,
      ObjectResolver resolver,
      boolean streamIsBuffered,
      boolean externStringLimits) {
    useStringUtf8Over =
        Integer.getInteger("useStringUtf8Over", STRING_UTF_OVER_SOLR_UTIL_SZ_CUTOFF);
    this.resolver = resolver;
    this.externStringLimits = externStringLimits;
    initWrite(os, streamIsBuffered);
  }

  public JavaBinCodec(OutputStream os, ObjectResolver resolver, boolean streamIsBuffered) {
    this(os, resolver, streamIsBuffered, true);
  }

  public JavaBinCodec(ObjectResolver resolver) {
    this(resolver, null);
  }

  public JavaBinCodec(ObjectResolver resolver, boolean externStringLimits) {
    this(resolver, null, externStringLimits);
  }

  public JavaBinCodec setWritableDocFields(WritableDocFields writableDocFields){
    this.writableDocFields = writableDocFields;
    return this;

  }

  public JavaBinCodec(
      ObjectResolver resolver, StringCache stringCache, boolean externStringLimits) {
    this.resolver = resolver;
    this.stringCache = stringCache;
    this.externStringLimits = externStringLimits;
    useStringUtf8Over =
        Integer.getInteger("useStringUtf8Over", STRING_UTF_OVER_SOLR_UTIL_SZ_CUTOFF);
  }

  public JavaBinCodec(ObjectResolver resolver, StringCache stringCache) {
    this(resolver, stringCache, true);
  }

  public ObjectResolver getResolver() {
    return resolver;
  }

  /**
   * Marshal the given Object to the given OutputStream.
   *
   * @param nl               the Object to marshal
   * @param os               the OutputStream to marshal to
   * @throws IOException the io exception
   */
  public void marshal(Object nl, OutputStream os) throws IOException {
    marshal(nl, os, false);
  }

  /**
   * Marshal the given Object to the given OutputStream.
   *
   * @param nl               the Object to marshal
   * @param os               the OutputStream to marshal to
   * @param streamIsBuffered if true, no additional buffering for the OutputStream will be considered necessary.
   * @throws IOException the io exception
   */
  public void marshal(Object nl, OutputStream os, boolean streamIsBuffered) throws IOException {
    try {
      initWrite(os, streamIsBuffered);
      writeVal(nl);
    } finally {
      alreadyMarshalled = true;
      daos.flushBuffer();
    }
  }

  protected void initWrite(OutputStream os, boolean streamIsBuffered) {
    assert !alreadyMarshalled;
    // streamIsBuffered = false;
    init(streamIsBuffered ? new JavaBinOutputStream(os, null, 0) : new JavaBinOutputStream(os));
    try {
      daos.writeByte(VERSION);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }


  /** expert: sets a new output stream */
  public void init(JavaBinOutputStream os) {
    daos = os;
  }

  public void init(FastOutputStream os) {
    daos = JavaBinOutputStream.wrap(os);
  }

  byte version;

  public Object unmarshal(byte[] buf) throws IOException {
    FastInputStream dis = initRead(buf);
    return readVal(dis);
  }

  public Object unmarshal(InputStream is) throws IOException {
    FastInputStream dis = initRead(is);
    return readVal(dis);
  }

  public Object unmarshal(InputStream is, boolean streamIsBuffered) throws IOException {
    FastInputStream dis = initRead(is, streamIsBuffered);
    return readVal(dis);
  }


  protected FastInputStream initRead(InputStream is) throws IOException {
    return initRead(is, false);
  }

  protected FastInputStream initRead(InputStream is, boolean streamIsBuffered) throws IOException {
    assert !alreadyUnmarshalled;
    //streamIsBuffered = false;
    FastInputStream dis = streamIsBuffered ? AlreadyBufferedInputStream.wrap(is) : FastInputStream.wrap(is);
    return _init(dis);
  }

  protected FastInputStream initRead(byte[] buf) throws IOException {
    assert !alreadyUnmarshalled;
    FastInputStream dis = AlreadyBufferedInputStream.wrap(new ByteArrayInputStream(buf));
    return _init(dis);
  }

  protected FastInputStream _init(FastInputStream dis)
      throws IOException, InvalidEncodingException {
    version = dis.readByte();
    if (version != VERSION) {
      throw new InvalidEncodingException(
          "Invalid version (expected "
              + VERSION
              + ", but "
              + version
              + ") or the data in not in 'javabin' format");
    }

    alreadyUnmarshalled = true;
    return dis;
  }


  public SimpleOrderedMap<Object> readOrderedMap(DataInputInputStream dis) throws IOException {
    int sz = readSize(dis);
    SimpleOrderedMap<Object> nl = new SimpleOrderedMap<>(sz);
    for (int i = 0; i < sz; i++) {
      String name = (String) readVal(dis);
      Object val = readVal(dis);
      nl.add(name, val);
    }
    return nl;
  }

  public NamedList<Object> readNamedList(DataInputInputStream dis) throws IOException {
    int sz = readSize(dis);
    NamedList<Object> nl = new NamedList<>(sz);
    for (int i = 0; i < sz; i++) {
      String name = (String) readVal(dis);
      Object val = readVal(dis);
      nl.add(name, val);
    }
    return nl;
  }

  public void writeNamedList(NamedList<?> nl) throws IOException {
    final int size = nl.size();
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

  public Object readVal(DataInputInputStream dis) throws IOException {
    tagByte = dis.readByte();
    return readObject(dis);
  }

  protected Object readObject(DataInputInputStream dis) throws IOException {
    // this method is broken up just a bit to (checkLessCommonTypes) to get
    // the method size under the limit for inlining by the C2 compiler


    // try type + size in single byte
    switch (tagByte >>> 5) {
      case STR >>> 5:
        return readStr(dis, stringCache, readStringAsCharSeq);
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

    // if ((tagByte & 0xe0) == 0) {
    // if top 3 bits are clear, this is a normal tag

    switch (tagByte) {
      case INT:
        return dis.readInt();
      case LONG:
        return dis.readLong();
      case BOOL_TRUE:
        return Boolean.TRUE;
      case BOOL_FALSE:
        return Boolean.FALSE;
      case DATE:
        return new Date(dis.readLong());
      case SOLRDOC:
        return readSolrDocument(dis);
      case SOLRDOCLST:
        return readSolrDocumentList(dis);
      case SOLRINPUTDOC:
        return readSolrInputDocument(dis);
      case MAP:
        return readMap(dis);
      case MAP_ENTRY:
        return readMapEntry(dis);
      case MAP_ENTRY_ITER:
        return readMapIter(dis);
    }

    return checkLessCommonTypes(dis);
  }

  private Object readTagAndStringOrSolrDocument(DataInputInputStream dis) throws IOException {
    tagByte = dis.readByte();
    return readStringOrSolrDocument(dis);
  }

  private Object readStringOrSolrDocument(DataInputInputStream dis) throws IOException {
    if (tagByte >>> 5 == STR >>> 5) {
      return readStr(dis, stringCache, readStringAsCharSeq);
    } else if (tagByte >>> 5 == EXTERN_STRING >>> 5) {
      return readExternString(dis);
    }

    switch (tagByte) {
      case SOLRDOC:
        return readSolrDocument(dis);
      case SOLRINPUTDOC:
        return readSolrInputDocument(dis);
      case FLOAT:
        return dis.readFloat();
    }

    throw new UnsupportedEncodingException("Unknown or unexpected type " + tagByte);
  }

  private Object checkLessCommonTypes(DataInputInputStream dis) throws IOException {
    switch (tagByte) {
      case NULL:
        return null;
      case DOUBLE:
        return dis.readDouble();
      case FLOAT:
        return dis.readFloat();
      case BYTE:
        return dis.readByte();
      case SHORT:
        return dis.readShort();
      case BYTEARR:
        return readByteArray(dis);
      case ITERATOR:
        return readIterator(dis);
      case END:
        return END_OBJ;
      case ENUM_FIELD_VALUE:
        return readEnumFieldValue(dis);
    }

    throw new UnsupportedEncodingException("Unknown type " + tagByte);
  }

  @SuppressWarnings({"rawtypes"})
  public boolean writeKnownType(Object val) throws IOException {
    while (true) {
      if (writePrimitive(val)) {
        return true;
      } else if (val instanceof NamedList) {
        writeNamedList((NamedList<?>) val);
        return true;
      } else if (val instanceof SolrInputField) {
        val = ((SolrInputField) val).getValue();
        continue;
      } else if (val instanceof SolrDocumentList) { // SolrDocumentList is a List, so must come before List check
        writeSolrDocumentList((SolrDocumentList) val);
        return true;
      } else if (val instanceof SolrDocument) {
        //this needs special treatment to know which fields are to be written
        writeSolrDocument((SolrDocument) val);
        return true;
      } else if (val instanceof SolrInputDocument) {
        writeSolrInputDocument((SolrInputDocument) val);
        return true;
      } else if (val instanceof Iterator) {
        writeIterator((Iterator) val);
        return true;
      } else if (writeLessCommonPrimitive(val)) return true;

      return writeLessCommonKnownType(val);
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private boolean writeLessCommonKnownType(Object val) throws IOException {
    if (val instanceof Map.Entry) {
      writeMapEntry((Map.Entry)val);
      return true;
    }
    else if (val instanceof MapWriter) {
      writeMap((MapWriter) val);
      return true;
    }
    else if (val instanceof Collection) {
      writeArray((Collection) val);
      return true;
    }
    else if (val instanceof Map) {
      writeMap((Map) val);
      return true;
    }
    else if (val instanceof IteratorWriter) {
      writeIterator((IteratorWriter) val);
      return true;
    }
    else if (val instanceof Object[]) {
      writeArray((Object[]) val);
      return true;
    }
    else if (val instanceof Path) {
      writeStr(((Path) val).toAbsolutePath().toString());
      return true;
    }
    else if (val instanceof Iterable) {
      writeIterator(((Iterable) val).iterator());
      return true;
    }
    else if (val instanceof EnumFieldValue) {
      writeEnumFieldValue((EnumFieldValue) val);
      return true;
    }
    else if (val instanceof MapSerializable) {
      //todo find a better way to reuse the map more efficiently
      writeMap(((MapSerializable) val).toMap(new NamedList().asShallowMap()));
      return true;
    }
    else if (val instanceof AtomicInteger) {
      writeInt(((AtomicInteger) val).get());
      return true;
    }
    else if (val instanceof AtomicLong) {
      writeLong(((AtomicLong) val).get());
      return true;
    }
    else if (val instanceof AtomicBoolean) {
      writeBoolean(((AtomicBoolean) val).get());
      return true;
    }
    return false;
  }

  private static class MapEntry implements Entry<Object, Object> {

    private final Object key;
    private final Object value;

    public MapEntry(Object key, Object value) {
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
      return "MapEntry[" + key + ':' + value + ']';
    }

    @DoNotCall
    @Override
    public final Object setValue(Object value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int hashCode() {
      int result = 31;
      result *= 31 + key.hashCode();
      result *= 31 + value.hashCode();
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
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
    private final JavaBinCodec javaBinCodec;

    public BinEntryWriter(JavaBinCodec javaBinCodec) {
      this.javaBinCodec = javaBinCodec;
    }

    @Override
    public MapWriter.EntryWriter put(CharSequence k, Object v) throws IOException {
      javaBinCodec.writeExternString(k);
      javaBinCodec.writeVal(v);
      return this;
    }

    @Override
    public MapWriter.EntryWriter put(CharSequence k, int v) throws IOException {
      javaBinCodec.writeExternString(k);
      javaBinCodec.writeInt(v);
      return this;
    }

    @Override
    public MapWriter.EntryWriter put(CharSequence k, long v) throws IOException {
      javaBinCodec.writeExternString(k);
      javaBinCodec.writeLong(v);
      return this;
    }

    @Override
    public MapWriter.EntryWriter put(CharSequence k, float v) throws IOException {
      javaBinCodec.writeExternString(k);
      javaBinCodec.writeFloat(v);
      return this;
    }

    @Override
    public MapWriter.EntryWriter put(CharSequence k, double v) throws IOException {
      javaBinCodec.writeExternString(k);
      javaBinCodec.writeDouble(v);
      return this;
    }

    @Override
    public MapWriter.EntryWriter put(CharSequence k, boolean v) throws IOException {
      javaBinCodec.writeExternString(k);
      javaBinCodec.writeBoolean(v);
      return this;
    }

    @Override
    public MapWriter.EntryWriter put(CharSequence k, CharSequence v) throws IOException {
      javaBinCodec.writeExternString(k);
      javaBinCodec.writeStr(v);
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
    writeTag(MAP_ENTRY_ITER);
    val.writeMap(ew);
    writeTag(END);
  }


  public void writeTag(byte tag) throws IOException {
    daos.writeByte(tag);
  }

  public void writeTag(byte tag, int size) throws IOException {
    if ((tag & 0xe0) != 0) {
      if (size < 0x1f) {
        daos.writeByte(tag | size);
      } else {
        daos.writeByte(tag | 0x1f);
        writeVInt(size - 0x1f, daos);
      }
    } else {
      daos.writeByte(tag);
      writeVInt(size, daos);
    }
  }

  public void writeByteArray(byte[] arr, int offset, int len) throws IOException {
    writeTag(BYTEARR, len);
    daos.write(arr, offset, len);
  }

  public static byte[] readByteArray(DataInputInputStream dis) throws IOException {
    byte[] arr = new byte[readVInt(dis)];
    dis.readFully(arr);
    return arr;
  }
  //use this to ignore the writable interface because , child docs will ignore the fl flag
  // is it a good design?
  private boolean ignoreWritable =false;
  private MapWriter.EntryWriter cew;

  public void writeSolrDocument(SolrDocument doc) throws IOException {
    List<SolrDocument> children = doc.getChildDocuments();
    int fieldsCount = 0;
    if(writableDocFields == null || writableDocFields.wantsAllFields() || ignoreWritable){
      fieldsCount = doc.size();
    } else {
      for (Entry<String, Object> e : doc) {
        if(toWrite(e.getKey())) fieldsCount++;
      }
    }
    int sz = fieldsCount + (children == null ? 0 : children.size());
    writeTag(SOLRDOC);
    writeTag(ORDERED_MAP, sz);
    if (cew == null) cew = new ConditionalKeyMapWriter.EntryWriterWrapper(ew, k -> toWrite(k.toString()));
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

  public SolrDocument readSolrDocument(DataInputInputStream dis) throws IOException {
    tagByte = dis.readByte();
    int size = readSize(dis);
    SolrDocument doc = new SolrDocument(new LinkedHashMap<>(size));
    for (int i = 0; i < size; i++) {
      String fieldName;
      Object obj = readTagAndStringOrSolrDocument(dis); // could be a field name, or a child document
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

  public SolrDocumentList readSolrDocumentList(DataInputInputStream dis) throws IOException {

    tagByte = dis.readByte();
    @SuppressWarnings("unchecked")
    List<Object> list = readArray(dis, readSize(dis));

    tagByte = dis.readByte();
    @SuppressWarnings("unchecked")
    List<Object> l = readArray(dis, readSize(dis));
    SolrDocumentList solrDocs = new SolrDocumentList(l.size());
    solrDocs.setNumFound((Long) list.get(0));
    solrDocs.setStart((Long) list.get(1));
    solrDocs.setMaxScore((Float) list.get(2));
    if (list.size() > 3) { //needed for back compatibility
      solrDocs.setNumFoundExact((Boolean)list.get(3));
    }

    l.forEach(doc -> solrDocs.add((SolrDocument) doc));

    return solrDocs;
  }

  public void writeSolrDocumentList(SolrDocumentList docs)
      throws IOException {
    writeTag(SOLRDOCLST);
    List<Object> l = new ArrayList<>(4);
    l.add(docs.getNumFound());
    l.add(docs.getStart());
    l.add(docs.getMaxScore());
    l.add(docs.getNumFoundExact());
    writeArray(l);
    writeArray(docs);
  }

  public SolrInputDocument readSolrInputDocument(DataInputInputStream dis) throws IOException {
    int sz = readVInt(dis);
    float docBoost = (Float)readVal(dis);
    if (docBoost != 1f) {
      String message = "Ignoring document boost: " + docBoost + " as index-time boosts are not supported anymore";
      if (WARNED_ABOUT_INDEX_TIME_BOOSTS.compareAndSet(false, true)) {
        log.warn(message);
      } else {
        log.debug(message);
      }
    }
    SolrInputDocument sdoc = createSolrInputDocument(sz);
    for (int i = 0; i < sz; i++) {
      String fieldName;
      Object obj = readTagAndStringOrSolrDocument(dis); // could be a boost, a field name, or a child document
      if (obj instanceof Float) {
        float boost = (Float)obj;
        if (boost != 1f) {
          String message = IGNORING_FIELD_BOOST_AS_INDEX_TIME_BOOSTS_ARE_NOT_SUPPORTED_ANYMORE_BOOST + boost;
          if (WARNED_ABOUT_INDEX_TIME_BOOSTS.compareAndSet(false, true)) {
            log.warn(message);
          } else {
            log.debug(message);

          }
        }

        fieldName = (String) readTagAndStringOrSolrDocument(dis);
      } else if (obj instanceof SolrInputDocument) {
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
    writeFloat(1f); // document boost
    sdoc.writeMap(new ConditionalKeyMapWriter.EntryWriterWrapper(ew,IGNORECHILDDOCS));
    if (children != null) {
      for (SolrInputDocument child : children) {
        writeSolrInputDocument(child);
      }
    }
  }


  public Map<Object, Object> readMapIter(DataInputInputStream dis) throws IOException {
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
    return size < 0 ? new LinkedHashMap<>(2) : new LinkedHashMap<>(size);
  }

  public Map<Object,Object> readMap(DataInputInputStream dis)
      throws IOException {
    int sz = readVInt(dis);
    return readMap(dis, sz);
  }

  protected Map<Object, Object> readMap(DataInputInputStream dis, int sz) throws IOException {
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
    writeTag(ITERATOR);
    val.writeIter(itemWriter);
    writeTag(END);
  }

  public void writeIterator(Iterator<?> iter) throws IOException {
    writeTag(ITERATOR);
    while (iter.hasNext()) {
      writeVal(iter.next());
    }
    writeTag(END);
  }

  public List<Object> readIterator(DataInputInputStream fis) throws IOException {
    ArrayList<Object> l = new ArrayList<>(8);
    while (true) {
      Object o = readVal(fis);
      if (o == END_OBJ) break;
      l.add(o);
    }
    return l;
  }

  public void writeArray(List<?> l) throws IOException {
    writeTag(ARR, l.size());
    for (Object o : l) {
      writeVal(o);
    }
  }

  public void writeArray(Collection<?> coll) throws IOException {
    writeTag(ARR, coll.size());
    for (Object o : coll) {
      writeVal(o);
    }

  }

  public void writeArray(Object[] arr) throws IOException {
    writeTag(ARR, arr.length);
    int len = arr.length;
    for (Object o : arr) {
      writeVal(o);
    }
  }

  @SuppressWarnings({"unchecked"})
  public List<Object> readArray(DataInputInputStream dis) throws IOException {
    int sz = readSize(dis);
    return readArray(dis, sz);
  }

  @SuppressWarnings({"rawtypes"})
  protected List readArray(DataInputInputStream dis, int sz) throws IOException {
    ArrayList<Object> l = new ArrayList<>(sz);
    for (int i = 0; i < sz; i++) {
      l.add(readVal(dis));
    }
    return l;
  }

  /**
   * write {@link EnumFieldValue} as tag+int value+string value
   *
   * @param enumFieldValue to write
   */
  public void writeEnumFieldValue(EnumFieldValue enumFieldValue) throws IOException {
    writeTag(ENUM_FIELD_VALUE);
    writeInt(enumFieldValue.toInt());
    writeStr(enumFieldValue.toString());
  }

  public void writeMapEntry(Map.Entry<?,?> val) throws IOException {
    writeTag(MAP_ENTRY);
    writeVal(val.getKey());
    writeVal(val.getValue());
  }

  /**
   * read {@link EnumFieldValue} (int+string) from input stream
   * @param dis data input stream
   * @return {@link EnumFieldValue}
   */
  public EnumFieldValue readEnumFieldValue(DataInputInputStream dis) throws IOException {
    Integer intValue = (Integer) readVal(dis);
    String stringValue = (String) convertCharSeq (readVal(dis));
    return new EnumFieldValue(intValue, stringValue);
  }


  public Map.Entry<Object,Object> readMapEntry(DataInputInputStream dis) throws IOException {
    final Object key = readVal(dis);
    final Object value = readVal(dis);
    return new MapEntry(key, value);
  }

  /** write the string as tag+length, with length being the number of UTF-8 bytes */
  public void writeStr(CharSequence s) throws IOException {
    if (s == null) {
      writeTag(NULL);
      return;
    }
    if (s instanceof Utf8CharSequence) {
      writeUTF8Str((Utf8CharSequence) s);
      return;
    }
    int end = s.length();
    //  int maxSize = (int) (end * 1.1f);
    //  System.out.println("str len = " + end);
    //    if (end >= 4) {
    //
    //      CharsetEncoder charsetEncoder = UTF8_ENCODER.get();
    //      charsetEncoder.reset();
    //
    //    //  int estimatedSize = (int) (inBuffer.remaining() * 1.1f);
    //
    //      int sz = encodedLength(s);
    //
    //      int readSize = Math.min(sz, 262144);
    //
    //
    //
    //      //if (bytes == null || bytes.length < readSize) bytes = new byte[readSize];
    //      //ByteUtils.UTF16toUTF8(s, 0, end, bytes, 0);
    //
    //      CharBuffer inBuffer = CharBuffer.wrap(s.toString().toCharArray());
    //
    //      writeTag(STR, sz);
    //
    //      ByteBuffer outBuffer;
    //      if (bytes != null && readSize < bytes.length) {
    //        outBuffer = ByteBuffer.wrap(bytes);
    //      } else {
    //        outBuffer = ByteBuffer.allocate(readSize);
    //        bytes = outBuffer.array();
    //      }
    //
    //      while (true) {
    //        CoderResult cr = (inBuffer.hasRemaining() ?
    //            charsetEncoder.encode(inBuffer, outBuffer, true) : CoderResult.UNDERFLOW);
    //        if (cr.isUnderflow()) {
    //          cr = charsetEncoder.flush(outBuffer);
    //        }
    //        if (cr.isUnderflow()) {
    //          break;
    //        }
    //        if (cr.isOverflow()) {
    //          outBuffer.flip();
    //          daos.write(outBuffer.array(), 0, outBuffer.limit());
    //          int maximumSize = inBuffer.remaining() * 3;
    //
    //          if (outBuffer.capacity() < maximumSize) {
    //            outBuffer = ByteBuffer.allocate(maximumSize);
    //          } else {
    //            outBuffer.clear();
    //          }
    //        }
    //      }
    //      if (outBuffer.position() > 0) {
    //        outBuffer.flip();
    //        daos.write(outBuffer.array(), 0, outBuffer.limit());
    //      }
    //
    //      return;
    ////      //  byte[] stringBytes = ((String)s).getBytes("UTF-8"); // important to use String
    // param here
    ////      System.out.println("str  = " + s);
    ////      int len = Utf8.encodedLength(s);
    ////      CharBuffer cb = CharBuffer.wrap(s);
    ////      writeTag(STR, len);
    ////      while (true) {
    ////        CoderResult result = ENCODER.encode(cb, byteBuffer, true);
    ////        if (result.OVERFLOW.isOverflow()) {
    ////          byteBuffer.flip();
    ////          daos.write(byteBuffer.array(), 0, byteBuffer.limit());
    ////          byteBuffer.clear();
    ////
    ////        } else if (result.isError()) {
    ////          throw new RuntimeException(result.toString());
    ////        } else {
    ////
    ////          break;
    ////        }
    ////
    ////      }
    ////
    ////      byteBuffer.flip();
    ////      daos.write(byteBuffer.array(), 0, byteBuffer.limit());
    ////      byteBuffer.clear();
    ////      ENCODER.flush(byteBuffer);
    ////      byteBuffer.flip();
    ////      daos.write(byteBuffer.array(), 0, byteBuffer.limit());
    ////      //ENCODER.reset();
    ////
    ////
    ////      byteBuffer.clear();
    //    }
    //    else {
    //
    //    long allocatedMemory =
    //        (Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory());
    //    long presumableFreeMemory = Runtime.getRuntime().maxMemory() - allocatedMemory;
    //
    //    System.out.println("String size is " + end + " presumed free mem is " +
    // presumableFreeMemory);
    if (end > useStringUtf8Over) {
      //  System.out.println("sz:" + s.length());

        if (end > 262144) {
//          long allocatedMemory = (Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory());
//          long presumableFreeMemory = Runtime.getRuntime().maxMemory() - allocatedMemory;
//          if (presumableFreeMemory < end * 6) {
            int sz = encodedLength(s);
            int readSize = Math.min(sz, 262144);
            if (bytes == null || bytes.length < readSize) bytes = new byte[readSize];

            writeTag(STR, sz);
            daos.flushBuffer();
        ByteUtils.writeUTF16toUTF8(s, 0, end, daos.getOutPut(), bytes);
            return;
       //   }
        }

      byte[] stringBytes = s.toString().getBytes(UTF_8);
      //  ByteBuffer buffer = StandardCharsets.UTF_8.encode((String) s);
      // byte[] stringBytes = buffer.array();

      writeTag(STR, stringBytes.length);
      daos.write(stringBytes);
    } else {
      int sz = encodedLength(s);
      if (bytes == null || bytes.length < sz) bytes = new byte[sz];
      ByteUtils.UTF16toUTF8(s, 0, end, bytes, 0);

      writeTag(STR, sz);
      daos.write(bytes, 0, sz);
    }
    //  }
//      if (bytes == null || bytes.length < maxSize) bytes = new byte[maxSize];
//      int sz = ByteUtils.UTF16toUTF8(s, 0, end, bytes, 0);
//      writeTag(STR, sz);
//      daos.write(bytes, 0, sz);
    // } else {
    //   writeStrLarge(s, end);
    //  }
  }

  /**
   * Returns the number of bytes in the UTF-8-encoded form of {@code sequence}. For a string, this
   * method is equivalent to {@code string.getBytes(UTF_8).length}, but is more efficient in both
   * time and space.
   *
   * <p>From Guava vs adding the complicated dependency, that begs for shading, for one method.
   *
   * @throws IllegalArgumentException if {@code sequence} contains ill-formed UTF-16 (unpaired
   *     surrogates)
   */
  public static int encodedLength(CharSequence sequence) {
    // Warning to maintainers: this implementation is highly optimized.
    int utf16Length = sequence.length();
    int utf8Length = utf16Length;
    int i = 0;

    // This loop optimizes for pure ASCII.
    while (i < utf16Length && sequence.charAt(i) < 0x80) {
      i++;
    }

    // This loop optimizes for chars less than 0x800.
    for (; i < utf16Length; i++) {
      char c = sequence.charAt(i);
      if (c < 0x800) {
        utf8Length += ((0x7f - c) >>> 31); // branch free!
      } else {
        utf8Length += encodedLengthGeneral(sequence, i);
        break;
      }
    }

    if (utf8Length < utf16Length) {
      // Necessary and sufficient condition for overflow because of maximum 3x expansion
      throw new IllegalArgumentException(
          "UTF-8 length does not fit in int: " + (utf8Length + (1L << 32)));
    }
    return utf8Length;
  }

  private static int encodedLengthGeneral(CharSequence sequence, int start) {
    int utf16Length = sequence.length();
    int utf8Length = 0;
    for (int i = start; i < utf16Length; i++) {
      char c = sequence.charAt(i);
      if (c < 0x800) {
        utf8Length += (0x7f - c) >>> 31; // branch free!
      } else {
        utf8Length += 2;
        // jdk7+: if (Character.isSurrogate(c)) {
        if (MIN_SURROGATE <= c && c <= MAX_SURROGATE) {
          // Check that we have a well-formed surrogate pair.
          if (Character.codePointAt(sequence, i) == c) {
            throw new IllegalArgumentException("Unpaired surrogate at index " + i + 1);
          }
          i++;
        }
      }
    }
    return utf8Length;
  }


  byte[] bytes;
  CharArr arr = new CharArr();
  private final StringBytes bytesRef = new StringBytes(null, 0, 0);

  public CharSequence readStr(DataInputInputStream dis) throws IOException {
    return readStr(dis, null, readStringAsCharSeq);
  }

  public CharSequence readStr(
      DataInputInputStream dis, StringCache stringCache, boolean readStringAsCharSeq)
      throws IOException {
    if (readStringAsCharSeq) {
      return readUtf8(dis);
    }
    int sz = readSize(dis);
    return _readStr(dis, stringCache, sz);
  }

  private CharSequence _readStr(DataInputInputStream dis, StringCache stringCache, int sz)
      throws IOException {
    if (bytes == null || bytes.length < sz) bytes = new byte[sz];
    dis.readFully(bytes, 0, sz);
    if (stringCache != null) {
      return stringCache.get(bytesRef.reset(bytes, 0, sz));
    } else {
      if (sz < useStringUtf8Over) {
        arr.reset();
        ByteUtils.UTF8toUTF16(bytes, 0, sz, arr);
        return arr.toString();
      }
      return new String(bytes, 0, sz, UTF_8); // important to use String param here
    }
  }

  /////////// code to optimize reading UTF8
  static final int MAX_UTF8_SZ = 1024 << 4;//too big strings can cause too much memory allocation
  private Function<ByteArrayUtf8CharSequence, String> stringProvider;
  private BytesBlock bytesBlock;

 // private ByteBuffer byteBuffer = ByteBuffer.allocate(819200);

  protected CharSequence readUtf8(DataInputInputStream dis) throws IOException {
    int sz = readSize(dis);
    return readUtf8(dis, sz);
  }

  protected CharSequence readUtf8(DataInputInputStream dis, int sz) throws IOException {
    ByteArrayUtf8CharSequence result = new ByteArrayUtf8CharSequence(null, 0, 0);
    if(dis.readDirectUtf8(result, sz)){
      result.stringProvider= getStringProvider();
      return result;
    }

    if (sz > MAX_UTF8_SZ) return _readStr(dis, null, sz);
    if (bytesBlock == null) bytesBlock = new BytesBlock(Math.max(sz, 1024 << 2));
    BytesBlock block = this.bytesBlock.expand(sz);
    dis.readFully(block.getBuf(), block.getStartPos(), sz);

    result.reset(block.getBuf(), block.getStartPos(), sz,null);
    result.stringProvider = getStringProvider();
    return result;
  }

  private Function<ByteArrayUtf8CharSequence, String> getStringProvider() {
    if (stringProvider == null) {
      stringProvider = new ByteArrayUtf8CharSequenceStringFunction();
    }
    return this.stringProvider;
  }

  public void writeInt(int val) throws IOException {
    if (val > 0) {
      int b = SINT | (val & 0x0f);

      if (val >= 0x0f) {
        b |= 0x10;
        daos.writeByte(b);
        writeVInt(val >>> 4, daos);
      } else {
        daos.writeByte(b);
      }

    } else {
      daos.writeByte(INT);
      daos.writeInt(val);
    }
  }

  public int readSmallInt(DataInputInputStream dis) throws IOException {
    int v = tagByte & 0x0F;
    if ((tagByte & 0x10) != 0) v = (readVInt(dis) << 4) | v;
    return v;
  }


  public void writeLong(long val) throws IOException {
    if ((val & 0xff00000000000000L) == 0) {
      int b = SLONG | ((int) val & 0x0f);
      if (val >= 0x0f) {
        b |= 0x10;
        daos.writeByte(b);
        writeVLong(val >>> 4, daos);
      } else {
        daos.writeByte(b);
      }
    } else {
      daos.writeByte(LONG);
      daos.writeLong(val);
    }
  }

  public long readSmallLong(DataInputInputStream dis) throws IOException {
    long v = tagByte & 0x0F;
    if ((tagByte & 0x10) != 0) v = (readVLong(dis) << 4) | v;
    return v;
  }

  public void writeFloat(float val) throws IOException {
    daos.writeByte(FLOAT);
    daos.writeFloat(val);
  }

  public boolean writePrimitive(Object val) throws IOException {

    if (val instanceof CharSequence) {
      writeStr((CharSequence) val);
      return true;
    } else if (val instanceof Number) {

      return writeNumber(val);

    } else if (val instanceof Date) {
      daos.writeByte(DATE);
      daos.writeLong(((Date) val).getTime());
      return true;
    } else if (val instanceof Boolean) {
      writeBoolean((Boolean) val);
      return true;
    }

    return false;
  }

  protected boolean writeLessCommonPrimitive(Object val) throws IOException {
    if (val == null) {
      daos.writeByte(NULL);
      return true;
    } else if (val instanceof byte[]) {
      writeByteArray((byte[]) val, 0, ((byte[]) val).length);
      return true;
    } else if (val instanceof ByteBuffer) {
      ByteBuffer buf = (ByteBuffer) val;
      writeByteArray(buf.array(), buf.arrayOffset() + buf.position(),buf.limit() - buf.position());
      return true;
    } else if (val == END_OBJ) {
      writeTag(END);
      return true;
    }
    return false;
  }

  private boolean writeNumber(Object val) throws IOException {
    if (val instanceof Integer) {
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
      daos.writeByte(BYTE);
      daos.writeByte(((Byte) val).intValue());
      return true;
    } else if (val instanceof Short) {
      daos.writeByte(SHORT);
      daos.writeShort(((Short) val).intValue());
      return true;
    }
    return false;
  }

  protected void writeBoolean(boolean val) throws IOException {
    if (val) daos.writeByte(BOOL_TRUE);
    else daos.writeByte(BOOL_FALSE);
  }

  protected void writeDouble(double val) throws IOException {
    daos.writeByte(DOUBLE);
    daos.writeDouble(val);
  }


  public void writeMap(Map<?,?> val) throws IOException {
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


  public int readSize(DataInputInputStream in) throws IOException {
    int sz = tagByte & 0x1f;
    if (sz == 0x1f) sz += readVInt(in);
    return sz;
  }


  /**
   * Special method for variable length int (copied from lucene). Usually used for writing the length of a
   * collection/array/map In most of the cases the length can be represented in one byte (length &lt; 127) so it saves 3
   * bytes/object
   *
   * @throws IOException If there is a low-level I/O error.
   */
  public static void writeVInt(int i, JavaBinOutputStream out) throws IOException {
    while ((i & ~0x7F) != 0) {
      out.writeByte((byte) ((i & 0x7f) | 0x80));
      i >>>= 7;
    }
    out.writeByte((byte) i);
  }

  /**
   * The counterpart for {@link #writeVInt(int, JavaBinOutputStream)}
   *
   * @throws IOException If there is a low-level I/O error.
   */
  public static int readVInt(DataInputInputStream in) throws IOException {
    byte b = in.readByte();
    int i = b & 0x7F;
    for (int shift = 7; (b & 0x80) != 0; shift += 7) {
      b = in.readByte();
      i |= (b & 0x7F) << shift;
    }
    return i;
  }


  public static void writeVLong(long i, JavaBinOutputStream out) throws IOException {
    while ((i & ~0x7F) != 0) {
      out.writeByte((byte) ((i & 0x7f) | 0x80));
      i >>>= 7;
    }
    out.writeByte((byte) i);
  }

  public static long readVLong(DataInputInputStream in) throws IOException {
    byte b = in.readByte();
    long i = b & 0x7F;
    for (int shift = 7; (b & 0x80) != 0; shift += 7) {
      b = in.readByte();
      i |= (long) (b & 0x7F) << shift;
    }
    return i;
  }

  private Map<String, Integer> stringsMap;

  private boolean mapFull;
  private List<CharSequence> stringsList;

  public void writeExternString(CharSequence s) throws IOException {
    if (s == null) {
      writeTag(NULL);
      return;
    }

    if (externStringLimits && (mapFull || s.length() > MAX_EXTERN_STRING_LENGTH)) {
      writeStr(s);
      return;
    }

    Integer idx = stringsMap == null ? null : stringsMap.get(s);
    if (idx == null) idx = ZERO;
    writeTag(EXTERN_STRING, idx);
    if (idx.equals(ZERO)) {
      writeStr(s);
      if (stringsMap == null) stringsMap = new HashMap<>(64, 0.5f);
      int sz = stringsMap.size();
      stringsMap.put(s.toString(), sz + 1);
      if (!externStringLimits && sz == MAX_EXTERN_STRING_CACHE_SZ) {
        mapFull = true;
      }
    }

  }

  public CharSequence readExternString(DataInputInputStream fis) throws IOException {
    int idx = readSize(fis);
    if (idx != 0) { // idx != 0 is the index of the extern string
      return stringsList.get(idx - 1);
    } else {// idx == 0 means it has a string value
      tagByte = fis.readByte();
      CharSequence s = readStr(fis, stringCache, false);
      if (s != null) s = s.toString();
      if (stringsList == null) stringsList = new ArrayList<>(8);
      stringsList.add(s);
      return s;
    }
  }


  public void writeUTF8Str(Utf8CharSequence utf8) throws IOException {
    writeTag(STR, utf8.size());
    daos.writeUtf8CharSeq(utf8);
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
        // make a copy because the buffer received may be changed later by the caller
        StringBytes copy =
            new StringBytes(
                Arrays.copyOfRange(b.bytes, b.offset, b.offset + b.length), 0, b.length);
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
      daos.flushBuffer();
    }
  }

  private static class ByteArrayUtf8CharSequenceStringFunction
      implements Function<ByteArrayUtf8CharSequence, String> {
    final CharArr charArr = new CharArr(8);

    @Override
    public String apply(ByteArrayUtf8CharSequence butf8cs) {
      synchronized (charArr) {
        charArr.reset();
        ByteUtils.UTF8toUTF16(butf8cs.buf, butf8cs.offset(), butf8cs.size(), charArr);
        return charArr.toString();
      }
    }
  }

  public static class InvalidEncodingException extends IOException {
    public InvalidEncodingException(String s) {
      super(s);
    }
  }
}
