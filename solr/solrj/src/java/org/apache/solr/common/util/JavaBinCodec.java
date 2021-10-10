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

import static org.apache.solr.common.util.ByteArrayUtf8CharSequence.convertCharSeq;

import com.google.errorprone.annotations.DoNotCall;
import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
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
import java.util.function.Function;
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
import org.eclipse.jetty.io.RuntimeIOException;
import org.noggit.CharArr;

/**
 * Defines a space-efficient serialization/deserialization format for transferring data.
 *
 * <p>JavaBinCodec has built in support many commonly used types. This includes primitive types
 * (boolean, byte, short, double, int, long, float), common Java containers/utilities (Date, Map,
 * Collection, Iterator, String, Object[], byte[]), and frequently used Solr types ({@link
 * NamedList}, {@link SolrDocument}, {@link SolrDocumentList}). Each of the above types has a pair
 * of associated methods which read and write that type to a stream. b
 *
 * <p>Classes that aren't supported natively can still be serialized/deserialized by providing an
 * {@link JavaBinCodec.ObjectResolver} object that knows how to work with the unsupported class.
 * This allows {@link JavaBinCodec} to be used to marshall/unmarshall arbitrary content.
 *
 * <p>NOTE -- {@link JavaBinCodec} instances cannot be reused for more than one marshall or
 * unmarshall operation.
 */
public class JavaBinCodec implements PushWriter {

  // WARNING! this class is heavily optimized and balancing a wide variety of use cases, data types
  // and sizes, and tradeoffs - please be thorough and careful with changes - not only is
  // performance considered across a large number of cases, but also the resource cost for that
  // performance

  // TODO: this should be two classes - an encoder and decoder

  private static final int BUFFER_SZ = 8192;

  // Solr encode / decode is only a win at fairly small values due to gc overhead
  // of copying byte arrays, until intrinsics/simd wins overcome
  private static final int MAX_SZ_BEFORE_STRING_UTF8_ENCODE_OVER_SOLR_DEFAULT = 72;
  private static final int MAX_SZ_BEFORE_STRING_UTF8_DECODE_OVER_SOLR_DEFAULT = 192;
  private static final int MAX_SZ_BEFORE_SLOWER_SOLR_UTF8_ENCODE_DECODE = 262144;
  private static final int MAX_SZ_BEFORE_STRING_UTF8_ENCODE_OVER_SOLR =
      Integer.getInteger(
          "maxSzBeforeStringUTF8EncodeOverSolr",
          MAX_SZ_BEFORE_STRING_UTF8_ENCODE_OVER_SOLR_DEFAULT);
  private static final int MAX_SZ_BEFORE_STRING_UTF8_DECODE_OVER_SOLR =
      Integer.getInteger(
          "maxSzBeforeStringUTF8DecodeOverSolr",
          MAX_SZ_BEFORE_STRING_UTF8_DECODE_OVER_SOLR_DEFAULT);

  private static final int MAX_STRING_SZ_TO_TRY_KEEPING_AS_UTF8_WO_CONVERT_BYTES =
      1024 << 4; // can cause too much memory allocation if too large

  private static final byte VERSION = 2;

  private static final Float FLOAT_1 = 1f;
  protected static final Object END_OBJ = new Object();

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
  public static final int OxOF = 0x0f;

  private final ObjectResolver resolver;
  private final StringCache stringCache;
  private WritableDocFields writableDocFields;
  private boolean alreadyMarshalled;
  private boolean alreadyUnmarshalled;
  protected boolean readStringAsCharSeq = false;

  protected byte tagByte;

  // extern string structures
  private int stringsCount;
  private Map<CharSequence, int[]> stringsMap;
  private ObjectArrayList<CharSequence> stringsList;

  public final BinEntryWriter ew = new BinEntryWriter();

  // caching objects
  protected byte[] bytes;
  private CharArr arr;
  private final StringBytes bytesRef = new StringBytes(null, 0, 0);

  // caching UTF-8 bytes and lazy conversion to UTF-16
  private Function<ByteArrayUtf8CharSequence, String> stringProvider;
  private BytesBlock bytesBlock;

  // internal stream wrapper classes
  private OutputStream out;
  private boolean isFastOutputStream;
  protected byte[] buf;
  protected int pos;

  protected InputStream in;
  protected int end;

  // protected long readFromStream; // number of bytes read from the underlying inputstream
  public JavaBinCodec() {
    resolver = null;
    writableDocFields = null;
    stringCache = null;
  }

  public JavaBinCodec setReadStringAsCharSeq(boolean flag) {
    readStringAsCharSeq = flag;
    return this;
  }

  /**
   * Instantiates a new Java bin codec to be used as a PushWriter.
   *
   * <p>Ensure that close() is called explicitly after use.
   *
   * @param os the OutputStream to marshal to
   * @param resolver a resolver to be used for resolving Objects
   */
  public JavaBinCodec(OutputStream os, ObjectResolver resolver) {
    this.resolver = resolver;
    stringCache = null;
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
  public JavaBinCodec(OutputStream os, ObjectResolver resolver, boolean streamIsBuffered) {
    this.resolver = resolver;
    stringCache = null;
    initWrite(os, streamIsBuffered);
  }

  public JavaBinCodec(ObjectResolver resolver) {
    this(resolver, null);
  }

  public JavaBinCodec setWritableDocFields(WritableDocFields writableDocFields) {
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

  /**
   * Marshals a given primitive or collection to an OutputStream.
   *
   * <p>Collections may be nested amd {@link NamedList} is a supported collection.
   *
   * @param object the primitive or Collection to marshal
   * @param outputStream the OutputStream to marshal to
   * @throws IOException on IO failure
   */
  public void marshal(Object object, OutputStream outputStream) throws IOException {
    marshal(object, outputStream, false);
  }

  /**
   * Marshals a given primitive or collection to an OutputStream.
   *
   * <p>Collections may be nested amd {@link NamedList} is a supported collection.
   *
   * @param object the primitive or Collection to marshal
   * @param outputStream the OutputStream to marshal to
   * @param streamIsBuffered a hint indicating whether the OutputStream is already buffered or not
   * @throws IOException on IO failure
   */
  public void marshal(Object object, OutputStream outputStream, boolean streamIsBuffered)
      throws IOException {
    try {
      initWrite(outputStream, streamIsBuffered);
      writeVal(this, object);
    } finally {
      alreadyMarshalled = true;
      flushBufferOS(this);
    }
  }

  private void initWrite(OutputStream os, boolean streamIsBuffered) {
    assert !alreadyMarshalled;

    if (streamIsBuffered) {
      initOutStream(os, null);
    } else {
      initOutStream(os);
    }
    try {
      writeByteToOS(this, VERSION);
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
  }

  public void init(OutputStream outputStream) {
    if (outputStream instanceof FastOutputStream) {
      initOutStream(outputStream, null);
    } else {
      initOutStream(outputStream);
    }
  }

  /**
   * Unmarshalls a primitive or collection from a byte array to an Object.
   *
   * @param buffer a byte buffer containing the marshaled Object
   * @return the unmarshalled Object
   * @throws IOException on IO failure
   */
  public Object unmarshal(byte[] buffer) throws IOException {
    initRead(buffer);
    return readVal(this);
  }

  /**
   * Unmarshalls a primitive or collection from an InputStream to an Object.
   *
   * @param inputStream an InputStream containing the marshaled Object
   * @return the unmarshalled Object
   * @throws IOException on IO failure
   */
  public Object unmarshal(InputStream inputStream) throws IOException {
    initRead(inputStream);
    return readVal(this);
  }

  public void initRead(InputStream is) throws IOException {
    assert !alreadyUnmarshalled;

    init(is);
    byte version = readByte(this);
    if (version != VERSION) {
      throw new InvalidEncodingException(
          "Invalid version (expected "
              + VERSION
              + ", but "
              + version
              + ") or the data in not in 'javabin' format");
    }

    alreadyUnmarshalled = true;
  }

  protected void initRead(byte[] buf) throws IOException {
    assert !alreadyUnmarshalled;
    InputStream dis = new BytesInputStream(buf);
    init(dis);
    byte version = readByte(this);
    if (version != VERSION) {
      throw new InvalidEncodingException(
          "Invalid version (expected "
              + VERSION
              + ", but "
              + version
              + ") or the data in not in 'javabin' format");
    }

    alreadyUnmarshalled = true;
  }

  public void init(InputStream dis) throws IOException {
    if (dis instanceof FastInputStream) {
      buf = null;
      pos = ((FastInputStream) dis).pos;
      end = ((FastInputStream) dis).end;
    } else {
      buf = new byte[BUFFER_SZ];
    }
    in = dis;
  }

  static SimpleOrderedMap<Object> readOrderedMap(JavaBinCodec javaBinCodec) throws IOException {
    int sz = readSize(javaBinCodec);
    SimpleOrderedMap<Object> nl = new SimpleOrderedMap<>(sz);
    for (int i = 0; i < sz; i++) {
      String name = (String) readVal(javaBinCodec);
      Object val = readVal(javaBinCodec);
      nl.add(name, val);
    }
    return nl;
  }

  public NamedList<Object> readNamedList() throws IOException {
    int sz = readSize(this);
    NamedList<Object> nl = new NamedList<>(sz);
    for (int i = 0; i < sz; i++) {
      String name = (String) readVal(this);
      Object val = readVal(this);
      nl.add(name, val);
    }
    return nl;
  }

  public static void writeNamedList(JavaBinCodec javaBinCodec, NamedList<?> nl) throws IOException {
    int size = nl.size();
    writeTag(javaBinCodec, nl instanceof SimpleOrderedMap ? ORDERED_MAP : NAMED_LST, size);
    nl.forEachIO(new NLWriterIOBiConsumer(javaBinCodec));
  }

  public static void writeVal(JavaBinCodec javaBinCodec, Object val) throws IOException {
    if (javaBinCodec.writeKnownType(val)) {
      return;
    } else {
      ObjectResolver resolver;
      if (val instanceof ObjectResolver) {
        resolver = (ObjectResolver) val;
      } else {
        resolver = javaBinCodec.resolver;
      }
      if (resolver != null) {
        Object tmpVal = resolver.resolve(val, javaBinCodec);
        if (tmpVal == null) return; // null means the resolver took care of it fully
        if (javaBinCodec.writeKnownType(tmpVal)) return;
      }
    }

    /* NOTE: if the user of this codec doesn't want this (e.g. UpdateLog) they can supply an
    ObjectResolver that does something else, like throw an exception.*/
    writeVal(javaBinCodec, val.getClass().getName() + ':' + val);
  }

  public static Object readVal(JavaBinCodec javaBinCodec) throws IOException {
    javaBinCodec.tagByte = readByte(javaBinCodec);
    return javaBinCodec.readObject();
  }

  protected Object readObject() throws IOException {

    /*
     NOTE: this method is broken up just a bit (ie checkLessCommonTypes) to get
     the method size under the limit for inlining by the C2 compiler

     FYI NOTE: if top 3 bits are clear, this is a normal tag
               i.e.  if ((tagByte & 0xe0) == 0)
    */

    // try type + size in single byte
    switch (tagByte >>> 5) {
      case STR >>> 5:
        return readStr(this, stringCache, readStringAsCharSeq);
      case SINT >>> 5:
        return readSmallInt(this);
      case SLONG >>> 5:
        return readSmallLong(this);
      case ARR >>> 5:
        return readArray(this);
      case ORDERED_MAP >>> 5:
        return readOrderedMap(this);
      case NAMED_LST >>> 5:
        return readNamedList();
      case EXTERN_STRING >>> 5:
        return readExternString(this);
    }

    switch (tagByte) {
      case INT:
        return readIntFromIS(this);
      case LONG:
        return readLongFromIS(this);
      case DATE:
        return new Date(readLongFromIS(this));
      case SOLRDOC:
        return readSolrDocument(this);
      case SOLRDOCLST:
        return readSolrDocumentList(this);
      case SOLRINPUTDOC:
        return readSolrInputDocument(this);
      case MAP:
        return readMap(this);
      case MAP_ENTRY:
        return readMapEntry(this);
      case MAP_ENTRY_ITER:
        return readMapIter(this);
    }

    return readLessCommonTypes(this);
  }

  private static long readLongFromIS(JavaBinCodec javaBinCodec) throws IOException {
    return (long) readUnsignedByte(javaBinCodec) << 56
        | (long) readUnsignedByte(javaBinCodec) << 48
        | (long) readUnsignedByte(javaBinCodec) << 40
        | (long) readUnsignedByte(javaBinCodec) << 32
        | (long) readUnsignedByte(javaBinCodec) << 24
        | (long) readUnsignedByte(javaBinCodec) << 16
        | (long) readUnsignedByte(javaBinCodec) << 8
        | readUnsignedByte(javaBinCodec);
  }

  private static int readIntFromIS(JavaBinCodec javaBinCodec) throws IOException {
    return readUnsignedByte(javaBinCodec) << 24
        | readUnsignedByte(javaBinCodec) << 16
        | readUnsignedByte(javaBinCodec) << 8
        | readUnsignedByte(javaBinCodec);
  }

  private static Object readTagThenStringOrSolrDocument(JavaBinCodec javaBinCodec)
      throws IOException {
    javaBinCodec.tagByte = readByte(javaBinCodec);
    return readStringOrSolrDocument(javaBinCodec);
  }

  private static Object readStringOrSolrDocument(JavaBinCodec javaBinCodec) throws IOException {
    if (javaBinCodec.tagByte >>> 5 == STR >>> 5) {
      return readStr(javaBinCodec, javaBinCodec.stringCache, javaBinCodec.readStringAsCharSeq);
    } else if (javaBinCodec.tagByte >>> 5 == EXTERN_STRING >>> 5) {
      return javaBinCodec.readExternString(javaBinCodec);
    }

    switch (javaBinCodec.tagByte) {
      case SOLRDOC:
        return javaBinCodec.readSolrDocument(javaBinCodec);
      case SOLRINPUTDOC:
        return javaBinCodec.readSolrInputDocument(javaBinCodec);
      case FLOAT:
        return javaBinCodec.readFloat();
    }

    throw new UnsupportedEncodingException("Unknown or unexpected type " + javaBinCodec.tagByte);
  }

  private Object readLessCommonTypes(JavaBinCodec javaBinCodec) throws IOException {
    switch (javaBinCodec.tagByte) {
      case BOOL_TRUE:
        return Boolean.TRUE;
      case BOOL_FALSE:
        return Boolean.FALSE;
      case NULL:
        return null;
      case DOUBLE:
        return readDouble();
      case FLOAT:
        return readFloat();
      case BYTE:
        return readByte(javaBinCodec);
      case SHORT:
        return readShort();
      case BYTEARR:
        return readByteArray(javaBinCodec);
      case ITERATOR:
        return javaBinCodec.readIterator(javaBinCodec);
      case END:
        return END_OBJ;
      case ENUM_FIELD_VALUE:
        return readEnumFieldValue(javaBinCodec);
    }

    throw new UnsupportedEncodingException("Unknown type " + javaBinCodec.tagByte);
  }

  @SuppressWarnings("rawtypes")
  private boolean writeKnownType(Object val) throws IOException {
    while (true) {
      if (writePrimitive(val)) {
        return true;
      } else if (val instanceof NamedList) {
        writeNamedList(this, (NamedList<?>) val);
        return true;
      } else if (val instanceof SolrInputField) {
        val = ((SolrInputField) val).getValue();
        continue;
      } else if (val
          instanceof
          SolrDocumentList) { // SolrDocumentList is a List, so must come before List check
        writeSolrDocumentList((SolrDocumentList) val);
        return true;
      } else if (val instanceof SolrDocument) {
        // this needs special treatment to know which fields are to be written
        writeSolrDocument((SolrDocument) val);
        return true;
      } else if (val instanceof SolrInputDocument) {
        writeSolrInputDocument((SolrInputDocument) val);
        return true;
      } else if (val instanceof Iterator) {
        writeIterator(this, (Iterator) val);
        return true;
      } else if (val instanceof Map.Entry) {
        writeMapEntry((Map.Entry) val);
        return true;
      } else if (val instanceof MapWriter) {
        writeMap((MapWriter) val);
        return true;
      } else if (val instanceof Map) {
        writeMap(this, (Map) val);
        return true;
      } else if (writeLessCommonPrimitive(this, val)) return true;

      return writeLessCommonKnownType(this, val);
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static boolean writeLessCommonKnownType(JavaBinCodec javaBinCodec, Object val)
      throws IOException {
    if (val instanceof Collection) {
      writeArray(javaBinCodec, (Collection) val);
      return true;
    } else if (val instanceof IteratorWriter) {
      javaBinCodec.writeIterator((IteratorWriter) val);
      return true;
    } else if (val instanceof Object[]) {
      writeArray(javaBinCodec, (Object[]) val);
      return true;
    } else if (val instanceof Path) {
      writeStr(javaBinCodec, ((Path) val).toAbsolutePath().toString());
      return true;
    } else if (val instanceof Iterable) {
      writeIterator(javaBinCodec, ((Iterable) val).iterator());
      return true;
    } else if (val instanceof EnumFieldValue) {
      javaBinCodec.writeEnumFieldValue((EnumFieldValue) val);
      return true;
    } else if (val instanceof MapSerializable) {
      // todo find a better way to reuse the map more efficiently
      writeMap(javaBinCodec, ((MapSerializable) val).toMap(new NamedList().asShallowMap()));
      return true;
    } else if (val instanceof AtomicInteger) {
      writeInt(javaBinCodec, ((AtomicInteger) val).get());
      return true;
    } else if (val instanceof AtomicLong) {
      writeLong(javaBinCodec, ((AtomicLong) val).get());
      return true;
    } else if (val instanceof AtomicBoolean) {
      writeBoolean(javaBinCodec, ((AtomicBoolean) val).get());
      return true;
    }
    return false;
  }

  private static class MapEntry implements Entry<Object, Object> {

    private final Object key;
    private final Object value;

    MapEntry(Object key, Object value) {
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
    @Deprecated
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
        return (key.equals(entry.getKey()) && value.equals(entry.getValue()));
      }
      return false;
    }
  }

  private static class NLWriterIOBiConsumer extends NamedList.IOBiConsumer {

    private final JavaBinCodec javaBinCodec;

    public NLWriterIOBiConsumer(JavaBinCodec javaBinCodec) {
      this.javaBinCodec = javaBinCodec;
    }

    @Override
    void accept(String n, Object v) throws IOException {
      javaBinCodec.writeExternString(n);
      writeVal(javaBinCodec, v);
    }
  }

  public final class BinEntryWriter extends MapWriter.EntryWriter {
    @Override
    public MapWriter.EntryWriter put(CharSequence k, Object v) throws IOException {
      writeExternString(k);
      writeVal(JavaBinCodec.this, v);
      return this;
    }

    @Override
    public MapWriter.EntryWriter put(CharSequence k, int v) throws IOException {
      writeExternString(k);
      writeInt(JavaBinCodec.this, v);
      return this;
    }

    @Override
    public MapWriter.EntryWriter put(CharSequence k, long v) throws IOException {
      writeExternString(k);
      writeLong(JavaBinCodec.this, v);
      return this;
    }

    @Override
    public MapWriter.EntryWriter put(CharSequence k, float v) throws IOException {
      writeExternString(k);
      writeFloat(v);
      return this;
    }

    @Override
    public MapWriter.EntryWriter put(CharSequence k, double v) throws IOException {
      writeExternString(k);
      writeDouble(JavaBinCodec.this, v);
      return this;
    }

    @Override
    public MapWriter.EntryWriter put(CharSequence k, boolean v) throws IOException {
      writeExternString(k);
      writeBoolean(JavaBinCodec.this, v);
      return this;
    }

    @Override
    public MapWriter.EntryWriter put(CharSequence k, CharSequence v) throws IOException {
      writeExternString(k);
      writeStr(JavaBinCodec.this, v);
      return this;
    }
  }

  public void writeMap(MapWriter val) throws IOException {
    writeTag(this, MAP_ENTRY_ITER);
    val.writeMap(ew);
    writeTag(this, END);
  }

  public static void writeTag(JavaBinCodec javaBinCodec, byte tag) throws IOException {
    writeByteToOS(javaBinCodec, tag);
  }

  public static void writeTag(JavaBinCodec javaBinCodec, byte tag, int size) throws IOException {
    if ((tag & 0xe0) != 0) {
      if (size < 0x1f) {
        writeByteToOS(javaBinCodec, tag | size);
      } else {
        writeByteToOS(javaBinCodec, tag | 0x1f);
        writeVInt(javaBinCodec, size - 0x1f);
      }
    } else {
      writeByteToOS(javaBinCodec, tag);
      writeVInt(javaBinCodec, size);
    }
  }

  public static void writeByteArray(JavaBinCodec javaBinCodec, byte[] arr, int offset, int len)
      throws IOException {
    writeTag(javaBinCodec, BYTEARR, len);
    writeToOS(javaBinCodec, arr, offset, len);
  }

  private byte[] readByteArray(JavaBinCodec javaBinCodec) throws IOException {
    byte[] arr = new byte[readVInt(javaBinCodec)];
    readFully(this, arr, 0, arr.length);
    return arr;
  }

  // children will return false in the predicate passed to EntryWriterWrapper e.g. NOOP
  private boolean isChildDoc = false;
  private MapWriter.EntryWriter cew;

  public void writeSolrDocument(SolrDocument doc) throws IOException {
    List<SolrDocument> children = doc.getChildDocuments();
    int fieldsCount = 0;
    if (writableDocFields == null || writableDocFields.wantsAllFields() || isChildDoc) {
      fieldsCount = doc.size();
    } else {
      for (Entry<String, Object> e : doc) {
        if (toWrite(e.getKey())) fieldsCount++;
      }
    }
    int sz = fieldsCount + (children == null ? 0 : children.size());
    writeTag(this, SOLRDOC);
    writeTag(this, ORDERED_MAP, sz);
    if (cew == null)
      cew = new ConditionalKeyMapWriter.EntryWriterWrapper(ew, k -> toWrite(k.toString()));
    doc.writeMap(cew);
    if (children != null) {
      try {
        isChildDoc = true;
        for (SolrDocument child : children) {
          writeSolrDocument(child);
        }
      } finally {
        isChildDoc = false;
      }
    }
  }

  private boolean toWrite(String key) {
    return writableDocFields == null || isChildDoc || writableDocFields.isWritable(key);
  }

  public SolrDocument readSolrDocument(JavaBinCodec javaBinCodec) throws IOException {
    tagByte = readByte(javaBinCodec);
    int size = readSize(javaBinCodec);
    SolrDocument doc = new SolrDocument(new LinkedHashMap<>(size));
    for (int i = 0; i < size; i++) {
      String fieldName;
      Object obj =
          readTagThenStringOrSolrDocument(
              javaBinCodec); // could be a field name, or a child document
      if (obj instanceof SolrDocument) {
        doc.addChildDocument((SolrDocument) obj);
        continue;
      } else {
        fieldName = (String) obj;
      }
      Object fieldVal = readVal(javaBinCodec);
      doc.setField(fieldName, fieldVal);
    }
    return doc;
  }

  public SolrDocumentList readSolrDocumentList(JavaBinCodec javaBinCodec) throws IOException {

    tagByte = readByte(javaBinCodec);
    @SuppressWarnings("unchecked")
    List<Object> list = readArray(javaBinCodec, readSize(javaBinCodec));

    tagByte = readByte(javaBinCodec);
    @SuppressWarnings("unchecked")
    List<Object> l = readArray(this, readSize(javaBinCodec));
    int sz = l.size();
    SolrDocumentList solrDocs = new SolrDocumentList(sz);
    solrDocs.setNumFound((Long) list.get(0));
    solrDocs.setStart((Long) list.get(1));
    solrDocs.setMaxScore((Float) list.get(2));
    if (list.size() > 3) { // needed for back compatibility
      solrDocs.setNumFoundExact((Boolean) list.get(3));
    }

    l.forEach(doc -> solrDocs.add((SolrDocument) doc));

    return solrDocs;
  }

  public void writeSolrDocumentList(SolrDocumentList docs) throws IOException {
    writeTag(this, SOLRDOCLST);
    List<Object> l = new ArrayList<>(4);
    l.add(docs.getNumFound());
    l.add(docs.getStart());
    l.add(docs.getMaxScore());
    l.add(docs.getNumFoundExact());
    writeArray(this, l);
    writeArray(this, docs);
  }

  public SolrInputDocument readSolrInputDocument(JavaBinCodec javaBinCodec) throws IOException {
    int sz = readVInt(javaBinCodec);
    readVal(javaBinCodec); // unused boost
    SolrInputDocument solrDoc = createSolrInputDocument(sz);
    for (int i = 0; i < sz; i++) {
      String fieldName;
      // we know we are expecting to read a String key, a child document (or a back compat boost)
      Object obj = readTagThenStringOrSolrDocument(javaBinCodec);
      if (obj instanceof Float) {
        // same as above, key, child doc, or back compat boost
        fieldName = (String) readTagThenStringOrSolrDocument(javaBinCodec);
      } else if (obj instanceof SolrInputDocument) {
        solrDoc.addChildDocument((SolrInputDocument) obj);
        continue;
      } else {
        fieldName = (String) obj;
      }
      Object fieldVal = readVal(javaBinCodec);
      solrDoc.setField(fieldName, fieldVal);
    }
    return solrDoc;
  }

  protected SolrInputDocument createSolrInputDocument(int sz) {
    return new SolrInputDocument(new LinkedHashMap<>(sz));
  }

  /** Writes a {@link SolrInputDocument}. */
  public void writeSolrInputDocument(SolrInputDocument sdoc) throws IOException {
    List<SolrInputDocument> children = sdoc.getChildDocuments();
    int sz = sdoc.size() + (children == null ? 0 : children.size());
    writeTag(this, SOLRINPUTDOC, sz);
    writeFloat(1f); // placeholder document boost for back compat
    sdoc.writeMap(
        new ConditionalKeyMapWriter.EntryWriterWrapper(
            ew, it -> !CommonParams.CHILDDOC.equals(it.toString())));
    if (children != null) {
      for (SolrInputDocument child : children) {
        writeSolrInputDocument(child);
      }
    }
  }

  static Map<Object, Object> readMapIter(JavaBinCodec javaBinCodec) throws IOException {
    Map<Object, Object> m = javaBinCodec.newMap(-1);
    for (; ; ) {
      Object key = readVal(javaBinCodec);
      if (key == END_OBJ) break;
      Object val = readVal(javaBinCodec);
      m.put(key, val);
    }
    return m;
  }

  /**
   * Creates new Map implementations for unmarshalled Maps.
   *
   * @param size the expected size or -1 for unknown size
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  protected Map<Object, Object> newMap(int size) {
    // open addressing with linear probing
    return size < 0
        ? new Object2ObjectLinkedOpenHashMap(16, 0.75f)
        : new Object2ObjectLinkedOpenHashMap(size, 0.5f);
  }
//  protected Map<Object, Object> newMap(int size) {
//    return size < 0
//        ? new HashMap<>(16, 0.75f)
//        : new HashMap<>((int) Math.max(2, nextPowerOfTwo((long)Math.ceil(size / .5f))), 0.5f);
//  }
//
//  public static long nextPowerOfTwo(long x) {
//    if (x == 0) return 1;
//    x--;
//    x |= x >> 1;
//    x |= x >> 2;
//    x |= x >> 4;
//    x |= x >> 8;
//    x |= x >> 16;
//    return (x | x >> 32) + 1;
//  }

  private static Map<Object, Object> readMap(JavaBinCodec javaBinCodec) throws IOException {
    int sz = readVInt(javaBinCodec);
    return readMap(javaBinCodec, sz);
  }

  static Map<Object, Object> readMap(JavaBinCodec javaBinCodec, int sz) throws IOException {
    Map<Object, Object> m = javaBinCodec.newMap(sz);
    for (int i = 0; i < sz; i++) {
      Object key = readVal(javaBinCodec);
      Object val = readVal(javaBinCodec);
      m.put(key, val);
    }
    return m;
  }

  private final ItemWriter itemWriter = new ObjectItemWriter();

  public void writeIterator(IteratorWriter val) throws IOException {
    writeTag(this, ITERATOR);
    val.writeIter(itemWriter);
    writeTag(this, END);
  }

  public static void writeIterator(JavaBinCodec javaBinCodec, Iterator<?> iter) throws IOException {
    writeTag(javaBinCodec, ITERATOR);
    while (iter.hasNext()) {
      writeVal(javaBinCodec, iter.next());
    }
    writeTag(javaBinCodec, END);
  }

  /**
   * Unmarshalls an Iterator from the DataInputInputStream into a List.
   *
   * @return a list containing the Objects from the unmarshalled Iterator
   * @throws IOException on IO failure
   */
  public List<Object> readIterator(JavaBinCodec javaBinCodec) throws IOException {
    List<Object> l = new ArrayList<>(8);
    while (true) {
      Object o = readVal(javaBinCodec);
      if (o == END_OBJ) break;
      l.add(o);
    }
    return l;
  }

  public static void writeArray(JavaBinCodec javaBinCodec, List<?> l) throws IOException {
    writeTag(javaBinCodec, ARR, l.size());
    for (Object o : l) {
      writeVal(javaBinCodec, o);
    }
  }

  public static void writeArray(JavaBinCodec javaBinCodec, Collection<?> coll) throws IOException {
    writeTag(javaBinCodec, ARR, coll.size());
    for (Object o : coll) {
      writeVal(javaBinCodec, o);
    }
  }

  public static void writeArray(JavaBinCodec javaBinCodec, Object[] arr) throws IOException {
    writeTag(javaBinCodec, ARR, arr.length);
    for (Object o : arr) {
      writeVal(javaBinCodec, o);
    }
  }

  @SuppressWarnings("unchecked")
  public static List<Object> readArray(JavaBinCodec javaBinCodec) throws IOException {
    int sz = readSize(javaBinCodec);
    return readArray(javaBinCodec, sz);
  }

  @SuppressWarnings("rawtypes")
  protected static List readArray(JavaBinCodec javaBinCodec, int sz) throws IOException {
    List<Object> l = new ArrayList<>(sz);
    for (int i = 0; i < sz; i++) {
      l.add(readVal(javaBinCodec));
    }
    return l;
  }

  /**
   * write {@link EnumFieldValue} as tag+int value+string value
   *
   * @param enumFieldValue to write
   */
  private void writeEnumFieldValue(EnumFieldValue enumFieldValue) throws IOException {
    writeTag(this, ENUM_FIELD_VALUE);
    writeInt(this, enumFieldValue.toInt());
    writeStr(this, enumFieldValue.toString());
  }

  private void writeMapEntry(Map.Entry<?, ?> val) throws IOException {
    writeTag(this, MAP_ENTRY);
    writeVal(this, val.getKey());
    writeVal(this, val.getValue());
  }

  static EnumFieldValue readEnumFieldValue(JavaBinCodec javaBinCodec) throws IOException {
    Integer intValue = (Integer) readVal(javaBinCodec);
    String stringValue = (String) convertCharSeq(readVal(javaBinCodec));
    return new EnumFieldValue(intValue, stringValue);
  }

  public static Map.Entry<Object, Object> readMapEntry(JavaBinCodec javaBinCodec)
      throws IOException {
    Object key = readVal(javaBinCodec);
    Object value = readVal(javaBinCodec);
    return new MapEntry(key, value);
  }

  public static void writeStr(JavaBinCodec javaBinCodec, CharSequence s) throws IOException {

    // writes the string as tag+length, with length being the number of UTF-8 bytes

    if (s == null) {
      writeTag(javaBinCodec, NULL);
      return;
    }
    if (s instanceof Utf8CharSequence) {
      writeUTF8Str(javaBinCodec, (Utf8CharSequence) s);
      return;
    }
    int end = s.length();

    if (end > MAX_SZ_BEFORE_STRING_UTF8_ENCODE_OVER_SOLR) {

      // however, when going too large, breaking up the conversion can allow
      // for much better scale and behavior regardless of it's performance
      if (end > MAX_SZ_BEFORE_SLOWER_SOLR_UTF8_ENCODE_DECODE) {

        // the previous internal length calc method we used was very costly - often
        // approaching what the actual conversion costs - this is from Guava
        int sz = ByteUtils.calcUTF16toUTF8LengthGuava(s);

        int readSize = Math.min(sz, MAX_SZ_BEFORE_SLOWER_SOLR_UTF8_ENCODE_DECODE);
        if (javaBinCodec.bytes == null || javaBinCodec.bytes.length < readSize)
          javaBinCodec.bytes = new byte[readSize];

        writeTag(javaBinCodec, STR, sz);
        flushBufferOS(javaBinCodec);
        ByteUtils.writeUTF16toUTF8(s, 0, end, javaBinCodec.out, javaBinCodec.bytes);
        return;
      }

      byte[] stringBytes = s.toString().getBytes(StandardCharsets.UTF_8);

      writeTag(javaBinCodec, STR, stringBytes.length);
      writeToOS(javaBinCodec, stringBytes);
    } else {
      int maxSize = end * ByteUtils.MAX_UTF8_BYTES_PER_CHAR;
      if (javaBinCodec.bytes == null || javaBinCodec.bytes.length < maxSize)
        javaBinCodec.bytes = new byte[maxSize];
      int sz = ByteUtils.UTF16toUTF8(s, 0, end, javaBinCodec.bytes, 0);

      writeTag(javaBinCodec, STR, sz);

      writeToOS(javaBinCodec, javaBinCodec.bytes, 0, sz);
    }
  }

  public static CharSequence readStr(JavaBinCodec javaBinCodec) throws IOException {
    return readStr(javaBinCodec, null, javaBinCodec.readStringAsCharSeq);
  }

  public static CharSequence readStr(
      JavaBinCodec javaBinCodec, StringCache stringCache, boolean readStringAsCharSeq)
      throws IOException {
    if (readStringAsCharSeq) {
      return readUtf8(javaBinCodec);
    }
    int sz = readSize(javaBinCodec);
    return readStr(javaBinCodec, stringCache, sz);
  }

  private static CharSequence readStr(JavaBinCodec javaBinCodec, StringCache stringCache, int sz)
      throws IOException {
    if (javaBinCodec.bytes == null || javaBinCodec.bytes.length < sz)
      javaBinCodec.bytes = new byte[sz];
    readFully(javaBinCodec, javaBinCodec.bytes, 0, sz);
    if (stringCache != null) {
      return stringCache.get(javaBinCodec.bytesRef.reset(javaBinCodec.bytes, 0, sz));
    } else {
      if (sz < MAX_SZ_BEFORE_STRING_UTF8_DECODE_OVER_SOLR
          || sz > MAX_SZ_BEFORE_SLOWER_SOLR_UTF8_ENCODE_DECODE) {
        if (javaBinCodec.arr == null) {
          javaBinCodec.arr = new CharArr(sz);
        } else {
          javaBinCodec.arr.reset();
        }
        ByteUtils.UTF8toUTF16(javaBinCodec.bytes, 0, sz, javaBinCodec.arr);

        return javaBinCodec.arr.toString();
      }
      return new String(javaBinCodec.bytes, 0, sz, StandardCharsets.UTF_8);
      /*
       NOTE: Until Java 9, you had to use "UTF-8" vs passing an Encoder or you would not
       get Encoder caching. However, as part of 'compact strings', UTF-8 was
       special cased, and now passing an Encoder is okay. Additionally, this path
       also now hits intrinsics for SIMD. It has been juiced beyond a developers
       reach even though it still almost always requires returning a defensive array copy.
       In Java 17+, this path is supposed to be even more efficient.
      */
    }
  }

  static CharSequence readUtf8(JavaBinCodec javaBinCodec) throws IOException {
    int sz = readSize(javaBinCodec);
    return readUtf8(javaBinCodec, sz);
  }

  private static CharSequence readUtf8(JavaBinCodec javaBinCodec, int sz) throws IOException {
    ByteArrayUtf8CharSequence result = new ByteArrayUtf8CharSequence(null, 0, 0);
    if (readDirectUtf8(javaBinCodec, result, sz)) {
      result.stringProvider = javaBinCodec.getStringProvider();
      return result;
    }

    if (sz > MAX_STRING_SZ_TO_TRY_KEEPING_AS_UTF8_WO_CONVERT_BYTES)
      return readStr(javaBinCodec, null, sz);

    if (javaBinCodec.bytesBlock == null)
      javaBinCodec.bytesBlock = new BytesBlock(Math.max(sz, 1024 << 2));

    BytesBlock block = javaBinCodec.bytesBlock.expand(sz);
    readFully(javaBinCodec, block.getBuf(), block.getStartPos(), sz);
    result.reset(block.getBuf(), block.getStartPos(), sz, null);
    result.stringProvider = javaBinCodec.getStringProvider();
    return result;
  }

  private Function<ByteArrayUtf8CharSequence, String> getStringProvider() {
    if (stringProvider == null) {
      stringProvider = new ByteArrayUtf8CharSequenceStringFunction();
    }
    return stringProvider;
  }

  public static void writeInt(JavaBinCodec javaBinCodec, int val) throws IOException {
    if (val > 0) {
      int b = SINT | (val & OxOF);
      if (val >= OxOF) {
        b |= 0x10;
        writeByteToOS(javaBinCodec, b);
        writeVInt(javaBinCodec, val >>> 4);
      } else {
        writeByteToOS(javaBinCodec, b);
      }

    } else {
      writeByteToOS(javaBinCodec, INT);
      writeIntToOS(javaBinCodec, val);
    }
  }

  public static int readSmallInt(JavaBinCodec javaBinCodec) throws IOException {
    int v = javaBinCodec.tagByte & OxOF;
    if ((javaBinCodec.tagByte & 0x10) != 0) v = (readVInt(javaBinCodec) << 4) | v;
    return v;
  }

  public static void writeLong(JavaBinCodec javaBinCodec, long val) throws IOException {
    if ((val & 0xff00000000000000L) == 0) {
      int b = SLONG | ((int) val & OxOF);
      if (val >= OxOF) {
        b |= 0x10;
        writeByteToOS(javaBinCodec, b);
        writeVLong(javaBinCodec, val >>> 4);
      } else {
        writeByteToOS(javaBinCodec, b);
      }
    } else {
      writeByteToOS(javaBinCodec, LONG);
      writeLongToOS(javaBinCodec, val);
    }
  }

  static long readSmallLong(JavaBinCodec javaBinCodec) throws IOException {
    long v = javaBinCodec.tagByte & OxOF;
    if ((javaBinCodec.tagByte & 0x10) != 0) v = (readVLong(javaBinCodec) << 4) | v;
    return v;
  }

  public void writeFloat(float val) throws IOException {
    writeByteToOS(this, FLOAT);
    writeFloatToOS(this, val);
  }

  public boolean writePrimitive(Object val) throws IOException {
    if (val instanceof CharSequence) {
      writeStr(this, (CharSequence) val);
      return true;
    } else if (val instanceof Integer) {
      writeInt(this, (Integer) val);
      return true;
    } else if (val instanceof Long) {
      writeLong(this, (Long) val);
      return true;
    } else if (val instanceof Float) {
      writeFloat((Float) val);
      return true;
    } else if (val instanceof Date) {
      writeByteToOS(this, DATE);
      writeLongToOS(this, ((Date) val).getTime());
      return true;
    } else if (val instanceof Boolean) {
      writeBoolean(this, (Boolean) val);
      return true;
    }
    return false;
  }

  private static boolean writeLessCommonPrimitive(JavaBinCodec javaBinCodec, Object val)
      throws IOException {
    if (val == null) {
      writeByteToOS(javaBinCodec, NULL);
      return true;
    } else if (val instanceof Double) {
      writeDouble(javaBinCodec, (Double) val);
      return true;
    } else if (val instanceof Short) {
      writeByteToOS(javaBinCodec, SHORT);
      writeShortToOS(javaBinCodec, ((Short) val).intValue());
      return true;
    } else if (val instanceof Byte) {
      writeByteToOS(javaBinCodec, BYTE);
      writeByteToOS(javaBinCodec, ((Byte) val).intValue());
      return true;
    } else if (val instanceof byte[]) {
      writeByteArray(javaBinCodec, (byte[]) val, 0, ((byte[]) val).length);
      return true;
    } else if (val instanceof ByteBuffer) {
      ByteBuffer buffer = (ByteBuffer) val;
      writeByteArray(
          javaBinCodec,
          buffer.array(),
          buffer.arrayOffset() + buffer.position(),
          buffer.limit() - buffer.position());
      return true;
    } else if (val == END_OBJ) {
      writeTag(javaBinCodec, END);
      return true;
    }
    return false;
  }

  protected static void writeBoolean(JavaBinCodec javaBinCodec, boolean val) throws IOException {
    if (val) writeByteToOS(javaBinCodec, BOOL_TRUE);
    else writeByteToOS(javaBinCodec, BOOL_FALSE);
  }

  protected static void writeDouble(JavaBinCodec javaBinCodec, double val) throws IOException {
    writeByteToOS(javaBinCodec, DOUBLE);
    writeDoubleToOS(javaBinCodec, val);
  }

  public static void writeMap(JavaBinCodec javaBinCodec, Map<?, ?> val) throws IOException {
    writeTag(javaBinCodec, MAP, val.size());
    if (val instanceof MapWriter) {
      ((MapWriter) val).writeMap(javaBinCodec.ew);
      return;
    }
    for (Map.Entry<?, ?> entry : val.entrySet()) {
      Object key = entry.getKey();
      if (key instanceof String) {
        javaBinCodec.writeExternString((CharSequence) key);
      } else {
        writeVal(javaBinCodec, key);
      }
      writeVal(javaBinCodec, entry.getValue());
    }
  }

  public static int readSize(JavaBinCodec javaBinCodec) throws IOException {
    int sz = javaBinCodec.tagByte & 0x1f;
    if (sz == 0x1f) sz += readVInt(javaBinCodec);
    return sz;
  }

  /**
   * Special method for variable length int (copied from lucene). Usually used for writing the
   * length of a collection/array/map In most of the cases the length can be represented in one byte
   * (length &lt; 127) so it saves 3 bytes/object
   *
   * @throws IOException If there is a low-level I/O error.
   */
  private static void writeVInt(JavaBinCodec javaBinCodec, int i) throws IOException {
    // i = encodeZigZag32(i);
    while ((i & ~0x7F) != 0) {
      writeByteToOS(javaBinCodec, (byte) ((i & 0x7f) | 0x80));
      i >>>= 7;
    }
    writeByteToOS(javaBinCodec, (byte) i);
  }

  /**
   * The counterpart for {@link JavaBinCodec#writeVInt(JavaBinCodec, int)}
   *
   * @throws IOException If there is a low-level I/O error.
   */
  protected static int readVInt(JavaBinCodec javaBinCodec) throws IOException {
    byte b = readByte(javaBinCodec);
    int i = b & 0x7F;
    for (int shift = 7; (b & 0x80) != 0; shift += 7) {
      b = readByte(javaBinCodec);
      i |= (b & 0x7F) << shift;
    }
    // return decodeZigZag32(i);
    return i;
  }

  private static void writeVLong(JavaBinCodec javaBinCodec, long i) throws IOException {
    while ((i & ~0x7F) != 0) {
      writeByteToOS(javaBinCodec, (byte) ((i & 0x7f) | 0x80));
      i >>>= 7;
    }
    writeByteToOS(javaBinCodec, (byte) i);
  }

  private static long readVLong(JavaBinCodec javaBinCodec) throws IOException {
    byte b = readByte(javaBinCodec);
    long i = b & 0x7F;
    for (int shift = 7; (b & 0x80) != 0; shift += 7) {
      b = readByte(javaBinCodec);
      i |= (long) (b & 0x7F) << shift;
    }
    return i;
  }

  // zigzag avoids the always worst case penalty of negatives - maybe for version 3?
  private static int encodeZigZag32(int n) {
    // Note:  the right-shift must be arithmetic
    return (n << 1) ^ (n >> 31);
  }

  private static int decodeZigZag32(int n) {
    return (n >>> 1) ^ -(n & 1);
  }

  // cost for long zip encode/decode is measurably larger hit then int

  //  private static long encodeZigZag64(final long n) {
  //    // Note:  the right-shift must be arithmetic
  //    return (n << 1) ^ (n >> 63);
  //  }
  //
  //  private static long decodeZigZag64(final long n) {
  //    return (n >>> 1) ^ -(n & 1);
  //  }

  public void writeExternString(CharSequence str) throws IOException {
    if (str == null) {
      writeTag(this, NULL);
      return;
    }
    int idx = 0;
    if (stringsMap != null) {

      int[] idxArr = stringsMap.get(str);
      if (idxArr != null) {
        idx = idxArr[0];
      }
    }

    writeTag(this, EXTERN_STRING, idx);
    if (idx == 0) {
      writeStr(this, str);
      if (stringsMap == null) {
        stringsMap = new HashMap<>(32, 0.75f);
      }
      stringsMap.put(str, new int[] {++stringsCount});
    }
  }

  public CharSequence readExternString(JavaBinCodec javaBinCodec) throws IOException {
    int idx = readSize(javaBinCodec);
    if (idx != 0) { // idx > 0 is the index of the extern string
      return stringsList.get(idx - 1);
    } else { // idx == 0 means it has a string value
      tagByte = readByte(javaBinCodec);
      CharSequence str = readStr(this, stringCache, false).toString();
      if (stringsList == null) stringsList = new ObjectArrayList<>(32);
      stringsList.add(str);
      return str;
    }
  }

  public static void writeUTF8Str(JavaBinCodec javaBinCodec, Utf8CharSequence utf8)
      throws IOException {
    writeTag(javaBinCodec, STR, utf8.size());
    writeUtf8CharSeqToOS(javaBinCodec, utf8);
  }

  /**
   * Allows extension of {@link JavaBinCodec} to support serialization of arbitrary data types.
   *
   * <p>Implementors of this interface write a method to serialize a given object using an existing
   * {@link JavaBinCodec}
   */
  public interface ObjectResolver {
    /**
     * Examine and attempt to serialize the given object, using a {@link JavaBinCodec} to write it
     * to a stream.
     *
     * @param o the object to serialize.
     * @param codec used to actually serialize {@code o}.
     * @return the object {@code o} itself if it could not be serialized, or {@code null} if the
     *     whole object was successfully serialized.
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

        if (b.length < MAX_SZ_BEFORE_STRING_UTF8_DECODE_OVER_SOLR) {
          CharArr arr = new CharArr(64);
          ByteUtils.UTF8toUTF16(b.bytes, b.offset, b.length, arr);
          result = arr.toString();
        } else {
          result = new String(b.bytes, b.offset, b.length, StandardCharsets.UTF_8);
        }

        cache.put(copy, result);
      }
      return result;
    }
  }

  @Override
  public void close() throws IOException {
    if (out != null) {
      flushBufferOS(this);
    }
  }

  /*
   We do a low level optimization here. Low level code that does IO, especially that writes
   bytes, is very impacted by any additional overhead. In the best case, things get inlined.
   However, the best case only goes so far. Inlining has cut off caps, limitations, reversals when assumptions get invalidated, etc.

   Via measurement and inspection, we find this and related existing classes have not been entirely compiler friendly.

   To further help the situation, we pull the OutputStream wrapper layer class into JavaBinCodec itself - allowing no additional layers
   of inheritance, nor the opportunity for the class to be co-opted for other tasks or uses, thus ensuring efficient code and monomorphic call sites.

   Looking at how the output methods should be dispatched to, the rule of thumb is that interfaces are slowest to dispatch on, abstract classes are faster,
   isolated classes obviously a decent case, but at the top is the simple jump of a static method call.
   The single byte methods show the most gratitude.
  */

  // passing null for the buffer writes straight through to the stream - if writing to something
  // like a ByteArrayOutputStream, intermediate buffering should not be used
  private void initOutStream(OutputStream sink, byte[] tempBuffer) {
    out = sink;
    buf = tempBuffer;
    pos = 0;
    if (sink instanceof FastOutputStream) {
      isFastOutputStream = true;
      if (tempBuffer != null) {
        throw new IllegalArgumentException(
            "FastInputStream cannot pass a buffer to JavaBinInputStream as it will already buffer - pass null to write to the stream directly");
      }
    } else {
      isFastOutputStream = false;
    }
  }

  private void initOutStream(OutputStream w) {
    // match jetty output buffer
    initOutStream(w, new byte[BUFFER_SZ]);
  }

  private static void writeToOS(JavaBinCodec javaBinCodec, byte[] b) throws IOException {
    if (javaBinCodec.buf == null) {
      javaBinCodec.out.write(b, 0, b.length);
      return;
    }
    writeToOS(javaBinCodec, b, 0, b.length);
  }

  private static void writeToOS(JavaBinCodec javaBinCodec, byte b) throws IOException {
    if (javaBinCodec.buf == null) {
      javaBinCodec.out.write(b);
      return;
    }

    if (javaBinCodec.pos >= javaBinCodec.buf.length) {
      flushOS(javaBinCodec, javaBinCodec.buf, 0, javaBinCodec.buf.length);
      javaBinCodec.pos = 0;
    }
    javaBinCodec.buf[javaBinCodec.pos++] = b;
  }

  private static void writeToOS(JavaBinCodec javaBinCodec, byte[] arr, int off, int len)
      throws IOException {
    if (javaBinCodec.buf == null) {
      javaBinCodec.out.write(arr, off, len);
      return;
    }

    for (; ; ) {
      int space = javaBinCodec.buf.length - javaBinCodec.pos;

      if (len <= space) {
        System.arraycopy(arr, off, javaBinCodec.buf, javaBinCodec.pos, len);
        javaBinCodec.pos += len;
        return;
      } else if (len > javaBinCodec.buf.length) {
        if (javaBinCodec.pos > 0) {
          flushOS(javaBinCodec, javaBinCodec.buf, 0, javaBinCodec.pos); // flush
          javaBinCodec.pos = 0;
        }
        // don't buffer, just write to sink
        flushOS(javaBinCodec, arr, off, len);
        return;
      }

      // buffer is too big to fit in the free space, but
      // not big enough to warrant writing on its own.
      // write whatever we can fit, then flush and iterate.

      System.arraycopy(arr, off, javaBinCodec.buf, javaBinCodec.pos, space);
      flushOS(javaBinCodec, javaBinCodec.buf, 0, javaBinCodec.buf.length);
      javaBinCodec.pos = 0;
      off += space;
      len -= space;
    }
  }

  protected static void writeByteToOS(JavaBinCodec javaBinCodec, int b) throws IOException {
    if (javaBinCodec.buf == null) {
      javaBinCodec.out.write((byte) b);
      return;
    }

    if (javaBinCodec.pos >= javaBinCodec.buf.length) {
      flushOS(javaBinCodec, javaBinCodec.buf, 0, javaBinCodec.buf.length);
      javaBinCodec.pos = 0;
    }
    javaBinCodec.buf[javaBinCodec.pos++] = (byte) b;
  }

  private static void writeShortToOS(JavaBinCodec javaBinCodec, int v) throws IOException {
    writeToOS(javaBinCodec, (byte) (v >>> 8));
    writeToOS(javaBinCodec, (byte) v);
  }

  private static void writeIntToOS(JavaBinCodec javaBinCodec, int v) throws IOException {
    if (javaBinCodec.buf == null) {
      javaBinCodec.out.write((byte) (v >>> 24));
      javaBinCodec.out.write((byte) (v >>> 16));
      javaBinCodec.out.write((byte) (v >>> 8));
      javaBinCodec.out.write((byte) (v));
      javaBinCodec.pos += 4;
      return;
    }

    if (4 > javaBinCodec.buf.length - javaBinCodec.pos && javaBinCodec.pos > 0) {
      flushOS(javaBinCodec, javaBinCodec.buf, 0, javaBinCodec.pos);
      javaBinCodec.pos = 0;
    }

    //intFromByteArrayVarHandle.set(javaBinCodec.buf, javaBinCodec.pos, v); // can't - odd

    javaBinCodec.buf[javaBinCodec.pos] = (byte) (v >>> 24);
    javaBinCodec.buf[javaBinCodec.pos + 1] = (byte) (v >>> 16);
    javaBinCodec.buf[javaBinCodec.pos + 2] = (byte) (v >>> 8);
    javaBinCodec.buf[javaBinCodec.pos + 3] = (byte) (v);
    javaBinCodec.pos += 4;
  }

  protected static void writeLongToOS(JavaBinCodec javaBinCodec, long v) throws IOException {
    if (javaBinCodec.buf == null) {
      javaBinCodec.out.write((byte) (v >>> 56));
      javaBinCodec.out.write((byte) (v >>> 48));
      javaBinCodec.out.write((byte) (v >>> 40));
      javaBinCodec.out.write((byte) (v >>> 32));
      javaBinCodec.out.write((byte) (v >>> 24));
      javaBinCodec.out.write((byte) (v >>> 16));
      javaBinCodec.out.write((byte) (v >>> 8));
      javaBinCodec.out.write((byte) (v));
      javaBinCodec.pos += 8;
      return;
    }

    if (8 > (javaBinCodec.buf.length - javaBinCodec.pos) && javaBinCodec.pos > 0) {
      flushOS(javaBinCodec, javaBinCodec.buf, 0, javaBinCodec.pos);
      javaBinCodec.pos = 0;
    }
   // longFromByteArrayVarHandle.set(javaBinCodec.buf, javaBinCodec.pos, v); // can't - odd
    javaBinCodec.buf[javaBinCodec.pos] = (byte) (v >>> 56);
    javaBinCodec.buf[javaBinCodec.pos + 1] = (byte) (v >>> 48);
    javaBinCodec.buf[javaBinCodec.pos + 2] = (byte) (v >>> 40);
    javaBinCodec.buf[javaBinCodec.pos + 3] = (byte) (v >>> 32);
    javaBinCodec.buf[javaBinCodec.pos + 4] = (byte) (v >>> 24);
    javaBinCodec.buf[javaBinCodec.pos + 5] = (byte) (v >>> 16);
    javaBinCodec.buf[javaBinCodec.pos + 6] = (byte) (v >>> 8);
    javaBinCodec.buf[javaBinCodec.pos + 7] = (byte) (v);
    javaBinCodec.pos += 8;
  }

  private static void writeFloatToOS(JavaBinCodec javaBinCodec, float v) throws IOException {
    writeIntToOS(javaBinCodec, Float.floatToRawIntBits(v));
  }

  private static void writeDoubleToOS(JavaBinCodec javaBinCodec, double v) throws IOException {
    writeLongToOS(javaBinCodec, Double.doubleToRawLongBits(v));
  }

  /** Only flushes the buffer of the FastOutputStream, not that of the underlying stream. */
  private static void flushBufferOS(JavaBinCodec javaBinCodec) throws IOException {
    if (javaBinCodec.buf == null) {
      if (javaBinCodec.isFastOutputStream) {
        ((FastOutputStream) javaBinCodec.out).flushBuffer();
      }
      return;
    }

    if (javaBinCodec.pos > 0) {
      flushOS(javaBinCodec, javaBinCodec.buf, 0, javaBinCodec.pos);
      javaBinCodec.pos = 0;
    }
  }

  /** All writes to the sink will go through this method */
  private static void flushOS(JavaBinCodec javaBinCodec, byte[] buf, int offset, int len)
      throws IOException {
    javaBinCodec.out.write(buf, offset, len);
  }

  /** Copies a {@link Utf8CharSequence} without making extra copies */
  private static void writeUtf8CharSeqToOS(JavaBinCodec javaBinCodec, Utf8CharSequence utf8)
      throws IOException {
    if (javaBinCodec.buf == null) {
      utf8.write(javaBinCodec.out);
      return;
    }

    int start = 0;
    int totalWritten = 0;
    while (true) {
      int size = utf8.size();
      if (totalWritten >= size) break;
      if (javaBinCodec.pos >= javaBinCodec.buf.length) flushBufferOS(javaBinCodec);
      int sz = utf8.write(start, javaBinCodec.buf, javaBinCodec.pos);
      javaBinCodec.pos += sz;
      totalWritten += sz;
      start += sz;
    }
  }

  static boolean readDirectUtf8(
      JavaBinCodec javaBinCodec, ByteArrayUtf8CharSequence utf8, int len) {
    if (javaBinCodec.buf == null) {
      return ((FastInputStream) javaBinCodec.in).readDirectUtf8(utf8, len);
    }
    if (javaBinCodec.in != null || javaBinCodec.end < javaBinCodec.pos + len) return false;
    utf8.reset(javaBinCodec.buf, javaBinCodec.pos, len, null);
    javaBinCodec.pos = javaBinCodec.pos + len;
    return true;
  }

  static ByteBuffer readDirectByteBuffer(int sz) {
    return null;
  }

  public static int read(JavaBinCodec javaBinCodec) throws IOException {
    if (javaBinCodec.buf == null) {
      return javaBinCodec.in.read();
    }
    if (javaBinCodec.pos >= javaBinCodec.end) {
      // this will set end to -1 at EOF
      int result;
      if (javaBinCodec.in == null) {
        result = -1;
      } else {
        result = javaBinCodec.in.read(javaBinCodec.buf, 0, javaBinCodec.buf.length);
      }
      javaBinCodec.end = result;
      javaBinCodec.pos = 0;
      if (javaBinCodec.pos >= javaBinCodec.end) return -1;
    }
    return javaBinCodec.buf[javaBinCodec.pos++] & 0xff;
  }

  private static int readUnsignedByte(JavaBinCodec javaBinCodec) throws IOException {
    if (javaBinCodec.buf == null) {
      return ((FastInputStream) javaBinCodec.in).readUnsignedByte();
    }
    if (javaBinCodec.pos >= javaBinCodec.end) {
      // this will set end to -1 at EOF
      int result;
      if (javaBinCodec.in == null) {
        result = -1;
      } else {
        result = javaBinCodec.in.read(javaBinCodec.buf, 0, javaBinCodec.buf.length);
      }
      javaBinCodec.end = result;
      javaBinCodec.pos = 0;
      if (javaBinCodec.pos >= javaBinCodec.end) {
        throw new EOFException();
      }
    }
    return javaBinCodec.buf[javaBinCodec.pos++] & 0xff;
  }

  public static int read(JavaBinCodec javaBinCodec, byte[] b, int off, int len) throws IOException {
    if (javaBinCodec.buf == null) {
      return javaBinCodec.in.read(b, off, len);
    }
    int r = 0; // number of bytes we have read

    // first read from our buffer;
    if (javaBinCodec.end - javaBinCodec.pos > 0) {
      r = Math.min(javaBinCodec.end - javaBinCodec.pos, len);
      System.arraycopy(javaBinCodec.buf, javaBinCodec.pos, b, off, r);
      javaBinCodec.pos += r;
    }

    if (r == len) return r;

    // refill
    // amount left to read is >= buffer size
    if (len - r >= javaBinCodec.buf.length) {
      int ret;
      if (javaBinCodec.in == null) {
        ret = -1;
      } else {
        ret = javaBinCodec.in.read(b, off + r, len - r);
      }
      if (ret >= 0) {
        r += ret;
        return r;
      } else {
        // negative return code
        return r > 0 ? r : -1;
      }
    }

    // this will set end to -1 at EOF
    int result;
    if (javaBinCodec.in == null) {
      result = -1;
    } else {
      result = javaBinCodec.in.read(javaBinCodec.buf, 0, javaBinCodec.buf.length);
    }
    javaBinCodec.end = result;
    javaBinCodec.pos = 0;

    // read rest from our buffer
    if (javaBinCodec.end - javaBinCodec.pos > 0) {
      int toRead = Math.min(javaBinCodec.end - javaBinCodec.pos, len - r);
      System.arraycopy(javaBinCodec.buf, javaBinCodec.pos, b, off + r, toRead);
      javaBinCodec.pos += toRead;
      r += toRead;
      return r;
    }

    return r > 0 ? r : -1;
  }

  public void closeInputStream() throws IOException {
    in.close();
  }

  public static void readFully(JavaBinCodec javaBinCodec, byte[] b, int off, int len)
      throws IOException {
    if (javaBinCodec.buf == null) {
      ((FastInputStream) javaBinCodec.in).readFully(b, off, len);
      return;
    }
    while (len > 0) {
      int ret = read(javaBinCodec, b, off, len);
      off += ret;
      len -= ret;
    }
  }

  public static byte readByte(JavaBinCodec javaBinCodec) throws IOException {
    if (javaBinCodec.buf == null) {
      return ((FastInputStream) javaBinCodec.in).readByte();
    }
    if (javaBinCodec.pos >= javaBinCodec.end) {
      // this will set end to -1 at EOF
      int result;
      if (javaBinCodec.in == null) {
        result = -1;
      } else {
        result = javaBinCodec.in.read(javaBinCodec.buf, 0, javaBinCodec.buf.length);
      }
      javaBinCodec.end = result;
      javaBinCodec.pos = 0;
      if (javaBinCodec.pos >= javaBinCodec.end) throw new EOFException();
    }
    return javaBinCodec.buf[javaBinCodec.pos++];
  }

  protected short readShort() throws IOException {
    return (short) ((readUnsignedByte(this) << 8) | readUnsignedByte(this));
  }

  private static final VarHandle intFromByteArrayVarHandle =
      MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);
  private static final VarHandle longFromByteArrayVarHandle =
      MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);

  public static int readInt(JavaBinCodec javaBinCodec) throws IOException {
    if (javaBinCodec.end - javaBinCodec.pos >= 4) {
      int val = (int) intFromByteArrayVarHandle.get(javaBinCodec.buf, javaBinCodec.pos);
      javaBinCodec.pos += 4;
     return val;
    }
    return ((readUnsignedByte(javaBinCodec) << 24)
        | (readUnsignedByte(javaBinCodec) << 16)
        | (readUnsignedByte(javaBinCodec) << 8)
        | readUnsignedByte(javaBinCodec));
  }

  public static long readLong(JavaBinCodec javaBinCodec) throws IOException {
    if (javaBinCodec.end - javaBinCodec.pos >= 8) {
      long val = (long) longFromByteArrayVarHandle.get(javaBinCodec.buf, javaBinCodec.pos);
      javaBinCodec.pos += 8;
      return val;
    }
    return (((long) readUnsignedByte(javaBinCodec)) << 56)
        | (((long) readUnsignedByte(javaBinCodec)) << 48)
        | (((long) readUnsignedByte(javaBinCodec)) << 40)
        | (((long) readUnsignedByte(javaBinCodec)) << 32)
        | (((long) readUnsignedByte(javaBinCodec)) << 24)
        | (readUnsignedByte(javaBinCodec) << 16)
        | (readUnsignedByte(javaBinCodec) << 8)
        | (readUnsignedByte(javaBinCodec));
  }

  public float readFloat() throws IOException {
    return Float.intBitsToFloat(readInt(this));
  }

  public double readDouble() throws IOException {
    return Double.longBitsToDouble(readLong(this));
  }

  private static class ByteArrayUtf8CharSequenceStringFunction
      implements Function<ByteArrayUtf8CharSequence, String> {

    @Override
    public String apply(ByteArrayUtf8CharSequence butf8cs) {
      return new String(butf8cs.buf, butf8cs.offset, butf8cs.length, StandardCharsets.UTF_8);
    }
  }

  public static class InvalidEncodingException extends IOException {
    public InvalidEncodingException(String s) {
      super(s);
    }
  }

  private class ObjectItemWriter implements ItemWriter {

    @Override
    public ItemWriter add(Object o) throws IOException {
      writeVal(JavaBinCodec.this, o);
      return this;
    }

    @Override
    public ItemWriter add(int v) throws IOException {
      writeInt(JavaBinCodec.this, v);
      return this;
    }

    @Override
    public ItemWriter add(long v) throws IOException {
      writeLong(JavaBinCodec.this, v);
      return this;
    }

    @Override
    public ItemWriter add(float v) throws IOException {
      writeFloat(v);
      return this;
    }

    @Override
    public ItemWriter add(double v) throws IOException {
      writeDouble(JavaBinCodec.this, v);
      return this;
    }

    @Override
    public ItemWriter add(boolean v) throws IOException {
      writeBoolean(JavaBinCodec.this, v);
      return this;
    }
  }

}
