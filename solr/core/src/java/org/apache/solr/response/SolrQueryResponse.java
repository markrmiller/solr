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
package org.apache.solr.response;

import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import javax.servlet.http.HttpServletResponse;

import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.search.ReturnFields;
import org.apache.solr.search.SolrReturnFields;

/**
 * <code>SolrQueryResponse</code> is used by a query handler to return
 * the response to a query request.
 *
 * <p>
 * <a id="returnable_data"></a><b>Note On Returnable Data...</b><br>
 * A <code>SolrQueryResponse</code> may contain the following types of
 * Objects generated by the <code>SolrRequestHandler</code> that processed
 * the request.
 * </p>
 * <ul>
 *  <li>{@link String}</li>
 *  <li>{@link Integer}</li>
 *  <li>{@link Long}</li>
 *  <li>{@link Float}</li>
 *  <li>{@link Double}</li>
 *  <li>{@link Boolean}</li>
 *  <li>{@link Date}</li>
 *  <li>{@link org.apache.solr.search.DocList}</li>
 *  <li>{@link org.apache.solr.common.SolrDocument} (since 1.3)</li>
 *  <li>{@link org.apache.solr.common.SolrDocumentList} (since 1.3)</li>
 *  <li>{@link Map} containing any of the items in this list</li>
 *  <li>{@link NamedList} containing any of the items in this list</li>
 *  <li>{@link Collection} containing any of the items in this list</li>
 *  <li>Array containing any of the items in this list</li>
 *  <li>null</li>
 * </ul>
 * <p>
 * Other data types may be added to the SolrQueryResponse, but there is no guarantee
 * that QueryResponseWriters will be able to deal with unexpected types.
 * </p>
 *
 *
 * @since solr 0.9
 */
public class SolrQueryResponse {
  public static final String NAME = "response";
  public static final String RESPONSE_HEADER_PARTIAL_RESULTS_KEY = "partialResults";
  public static final String RESPONSE_HEADER_SEGMENT_TERMINATED_EARLY_KEY = "segmentTerminatedEarly";
  private static final String RESPONSE_HEADER_KEY = "responseHeader";
  private static final String RESPONSE_KEY = "response";

  /**
   * Container for user defined values
   * @see #getValues
   * @see #add
   * @see #setAllValues
   * @see <a href="#returnable_data">Note on Returnable Data</a>
   */
  protected volatile NamedList<Object> values = new SimpleOrderedMap<>();
  
  
/**
   * Container for storing information that should be logged by Solr before returning.
   */
  protected NamedList<Object> toLog = new SimpleOrderedMap<>();

  protected ReturnFields returnFields;
  
  /**
   * Container for storing HTTP headers. Internal Solr components can add headers to
   * this SolrQueryResponse through the methods: {@link #addHttpHeader(String, String)}
   * and {@link #setHttpHeader(String, String)}, or remove existing ones through 
   * {@link #removeHttpHeader(String)} and {@link #removeHttpHeaders(String)}. 
   * All these headers are going to be added to the HTTP response.
   */
  private final NamedList<String> headers = new SimpleOrderedMap<>();

  // error if this is set...
  protected Exception err;

  /**
   * Should this response be tagged with HTTP caching headers?
   */
  protected boolean httpCaching=true;
  
  /***
   // another way of returning an error
  int errCode;
  String errMsg;
  ***/

  public SolrQueryResponse() {
  }
  
  
  /**
   * Gets data to be returned in this response
   * @see <a href="#returnable_data">Note on Returnable Data</a>
   */
  public NamedList getValues() { return values; }

  /**
   * Sets data to be returned in this response
   * @see <a href="#returnable_data">Note on Returnable Data</a>
   */
  public void setAllValues(NamedList<Object> nameValuePairs) {
    values=nameValuePairs;
  }

  /**
   * Sets the document field names of fields to return by default when
   * returning DocLists
   */
  public void setReturnFields(ReturnFields fields) {
    returnFields=fields;
  }

  /**
   * Gets the document field names of fields to return by default when
   * returning DocLists
   */
  public ReturnFields getReturnFields() {
    if( returnFields == null ) {
      returnFields = new SolrReturnFields(); // by default return everything
    }
    return returnFields;
  }


  /**
   * Appends a named value to the list of named values to be returned.
   * @param name  the name of the value - may be null if unnamed
   * @param val   the value to add - also may be null since null is a legal value
   * @see <a href="#returnable_data">Note on Returnable Data</a>
   */
  public void add(String name, Object val) {
    values.add(name,val);
  }

  /**
   * Causes an error to be returned instead of the results.
   * 
   * In general, new calls to this method should not be added. In most cases
   * you should simply throw an exception and let it bubble out to 
   * RequestHandlerBase, which will set the exception thrown.
   */
  public void setException(Exception e) {
    err=e;
  }

  /**
   * Returns an Exception if there was a fatal error in processing the request.
   * Returns null if the request succeeded.
   */
  public Exception getException() {
    return err;
  }

  /** Set response header */
  public void addResponseHeader(NamedList<Object> header) {
    values.add(RESPONSE_HEADER_KEY, header);
  }

  /** Clear response header */
  public void removeResponseHeader() {
    values.remove(RESPONSE_HEADER_KEY);
  }

  /** Response header to be logged */
  public NamedList<Object> getResponseHeader() {
    @SuppressWarnings("unchecked")
    SimpleOrderedMap<Object> header = (SimpleOrderedMap<Object>) values.get(RESPONSE_HEADER_KEY);
    return header;
  }

  /** Set response */
  public void addResponse(Object response) {
    values.add(RESPONSE_KEY, response);
  }

  /** Return response */
  public Object getResponse() {
    return values.get(RESPONSE_KEY);
  }
  
  /** Add a value to be logged.
   * 
   * @param name name of the thing to log
   * @param val value of the thing to log
   */
  public void addToLog(String name, Object val) {
    toLog.add(name, val);
  }
  
  /** Get loggable items.
   * 
   * @return things to log
   */
  public NamedList<Object> getToLog() {
    return toLog;
  }

  /** Returns a string of the form "logid name1=value1 name2=value2 ..." */
  public String getToLogAsString(String logid) {
    StringBuilder sb = new StringBuilder(logid.length() + 1024);
    sb.append(logid);
    for (int i=0; i<toLog.size(); i++) {
      if (sb.length() > 0) {
        sb.append(' ');
      }
      String name = toLog.getName(i);
      Object val = toLog.getVal(i);
      if (name != null) {
        sb.append(name).append('=');
      }
      sb.append(val);
    }
    return sb.toString();
  }
  
  /**
   * Enables or disables the emission of HTTP caching headers for this response.
   * @param httpCaching true=emit caching headers, false otherwise
   */
  public void setHttpCaching(boolean httpCaching) {
    this.httpCaching=httpCaching;
  }
  
  /**
   * Should this response emit HTTP caching headers?
   * @return true=yes emit headers, false otherwise
   */
  public boolean isHttpCaching() {
    return this.httpCaching;
  }
  
  /**
   *
   * Sets a response header with the given name and value. This header
   * will be included in the HTTP response
   * If the header had already been set, the new value overwrites the
   * previous ones (all of them if there are multiple for the same name).
   * 
   * @param name the name of the header
   * @param value the header value  If it contains octet string,
   *    it should be encoded according to RFC 2047
   *    (http://www.ietf.org/rfc/rfc2047.txt)
   *
   * @see #addHttpHeader
   * @see HttpServletResponse#setHeader
   */
  public void setHttpHeader(String name, String value) {
    headers.removeAll(name);
    headers.add(name, value);
  }
 
  /**
   * Adds a response header with the given name and value. This header
   * will be included in the HTTP response
   * This method allows response headers to have multiple values.
   * 
   * @param name  the name of the header
   * @param value the additional header value   If it contains
   *    octet string, it should be encoded
   *    according to RFC 2047
   *    (http://www.ietf.org/rfc/rfc2047.txt)
   *
   * @see #setHttpHeader
   * @see HttpServletResponse#addHeader
   */
  public void addHttpHeader(String name, String value) {
     headers.add(name, value);
  }
 
  /**
   * Gets the value of the response header with the given name.
   * 
   * <p>If a response header with the given name exists and contains
   * multiple values, the value that was added first will be returned.</p>
   *
   * <p>NOTE: this runs in linear time (it scans starting at the
   * beginning of the list until it finds the first pair with
   * the specified name).</p>
   *
   * @param name the name of the response header whose value to return
   * @return the value of the response header with the given name,
   * or <code>null</code> if no header with the given name has been set
   * on this response
   */
  public String getHttpHeader(String name) {
    return headers.get(name);
  }

  /**
   * Gets the values of the response header with the given name.
   *
   * @param name the name of the response header whose values to return
   *
   * @return a (possibly empty) <code>Collection</code> of the values
   * of the response header with the given name
   *
   */     
  public Collection<String> getHttpHeaders(String name) {
    return headers.getAll(name);
  }

  /**
   * Removes a previously added header with the given name (only
   * the first one if multiple are present for the same name) 
   *
   * <p>NOTE: this runs in linear time (it scans starting at the
   * beginning of the list until it finds the first pair with
   * the specified name).</p>
   *
   * @param name the name of the response header to remove
   * @return the value of the removed entry or <code>null</code> if no 
   * value is found for the given header name 
   */
  public String removeHttpHeader(String name) {
    return headers.remove(name);
  }

  /**
   * Removes all previously added headers with the given name. 
   *
   * @param name the name of the response headers to remove
   * @return a <code>Collection</code> with all the values
   * of the removed entries. It returns <code>null</code> if no
   * entries are found for the given name
   */
  public Collection<String> removeHttpHeaders(String name) {
    return headers.removeAll(name);
  }

  /**
   * Returns a new iterator of response headers
   * @return a new Iterator instance for the response headers
   */
  public Iterator<Entry<String, String>> httpHeaders() {
    return headers.iterator();
  }

  public NamedList<String> getHeaders() {
    return headers;
  }
}
