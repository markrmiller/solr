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
package org.apache.solr.client.solrj.request;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.LBHttpSolrClient;
import org.apache.solr.client.solrj.impl.LBSolrClient;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.XML;

import static org.apache.solr.common.params.ShardParams._ROUTE_;

/**
 * 
 * 
 * @since solr 1.3
 */
public class UpdateRequest extends AbstractUpdateRequest {

  public static final String REPFACT = "rf";
  /**
   *   @deprecated Solr now always includes in the response the {@link #REPFACT}, this parameter
   *   doesn't need to be explicitly set
   */
  @Deprecated // SOLR-14034
  public static final String MIN_REPFACT = "min_rf";
  public static final String VER = "ver";
  public static final String OVERWRITE = "ow";

  public static final String COMMIT_WITHIN = "cw";
  private Object2ObjectMap<SolrInputDocument,Map<String,Object>> documents = null;
  private Iterator<SolrInputDocument> docIterator = null;
  private Object2ObjectMap<String,Map<String,Object>> deleteById = null;
  private ObjectList<String> deleteQuery = null;

  private boolean isLastDocInBatch = false;

  public UpdateRequest() {
    super(METHOD.POST, "/update");
  }
  
  public UpdateRequest(String url) {
    super(METHOD.POST, url);
  }
  
  // ---------------------------------------------------------------------------
  // ---------------------------------------------------------------------------
  
  /**
   * clear the pending documents and delete commands
   */
  public void clear() {
    if (documents != null) {
      documents.clear();
    }
    if (deleteById != null) {
      deleteById.clear();
    }
    if (deleteQuery != null) {
      deleteQuery.clear();
    }
    if (docIterator != null) {
      docIterator = null;
    }
    isLastDocInBatch = false;
  }
  
  // ---------------------------------------------------------------------------
  // ---------------------------------------------------------------------------

  /**
   * Add a SolrInputDocument to this request
   *
   * @throws NullPointerException if the document is null
   */
  public UpdateRequest add(final SolrInputDocument doc) {
    Objects.requireNonNull(doc, "Cannot add a null SolrInputDocument");
    if (documents == null) {
      documents = new Object2ObjectLinkedOpenHashMap<>(32, .5f);
    }
    documents.put(doc, null);
    return this;
  }

  public UpdateRequest add(String... fields) {
    return add(new SolrInputDocument(fields));
  }

  /**
   * Add a SolrInputDocument to this request
   * @param doc the document
   * @param overwrite true if the document should overwrite existing docs with the same id
   * @throws NullPointerException if the document is null
   */
  public UpdateRequest add(final SolrInputDocument doc, Boolean overwrite) {
    return add(doc, null, overwrite);
  }

  /**
   * Add a SolrInputDocument to this request
   * @param doc the document
   * @param commitWithin the time horizon by which the document should be committed (in ms)
   * @throws NullPointerException if the document is null
   */
  public UpdateRequest add(final SolrInputDocument doc, Integer commitWithin) {
    return add(doc, commitWithin, null);
  }

  /**
   * Add a SolrInputDocument to this request
   * @param doc the document
   * @param commitWithin the time horizon by which the document should be committed (in ms)
   * @param overwrite true if the document should overwrite existing docs with the same id
   * @throws NullPointerException if the document is null
   */
  public UpdateRequest add(final SolrInputDocument doc, Integer commitWithin, Boolean overwrite) {
    Objects.requireNonNull(doc, "Cannot add a null SolrInputDocument");
    if (documents == null) {
      documents = new Object2ObjectLinkedOpenHashMap<>(32, .5f);
    }
    Map<String,Object> params = new HashMap<>(2);
    if (commitWithin != null) params.put(COMMIT_WITHIN, commitWithin);
    if (overwrite != null) params.put(OVERWRITE, overwrite);
    
    documents.put(doc, params);
    
    return this;
  }

  /**
   * Add a collection of SolrInputDocuments to this request
   *
   * @throws NullPointerException if any of the documents in the collection are null
   */
  public UpdateRequest add(final Collection<SolrInputDocument> docs) {
    if (documents == null) {
      documents = new Object2ObjectLinkedOpenHashMap<>(32, .5f);
    }
    for (SolrInputDocument doc : docs) {
      Objects.requireNonNull(doc, "Cannot add a null SolrInputDocument");
      documents.put(doc, null);
    }
    return this;
  }
  
  public UpdateRequest deleteById(String id) {
    if (deleteById == null) {
      deleteById = new Object2ObjectLinkedOpenHashMap<>(32, .5f);
    }
    deleteById.put(id, null);
    return this;
  }

  public UpdateRequest deleteById(String id, String route) {
    return deleteById(id, route, null);
  }

  public UpdateRequest deleteById(String id, String route, Long version) {
    if (deleteById == null) {
      deleteById = new Object2ObjectLinkedOpenHashMap<>(32, .5f);
    }
    Map<String, Object> params = (route == null && version == null) ? null : new HashMap<>(1);
    if (version != null)
      params.put(VER, version);
    if (route != null)
      params.put(_ROUTE_, route);
    deleteById.put(id, params);
    return this;
  }


  public UpdateRequest deleteById(List<String> ids) {
    if (deleteById == null) {
      deleteById = new Object2ObjectLinkedOpenHashMap<>(32, .5f);
    }
    
    for (String id : ids) {
      deleteById.put(id, null);
    }
    
    return this;
  }
  
  public UpdateRequest deleteById(String id, Long version) {
    return deleteById(id, null, version);
  }
  
  public UpdateRequest deleteByQuery(String q) {
    if (deleteQuery == null) {
      deleteQuery = new ObjectArrayList<>();
    }
    deleteQuery.add(q);
    return this;
  }

  public UpdateRequest withRoute(String route) {
    if (params == null)
      params = new ModifiableSolrParams();
    params.set(_ROUTE_, route);
    return this;
  }

  public UpdateResponse commit(SolrClient client, String collection) throws IOException, SolrServerException {
    if (params == null)
      params = new ModifiableSolrParams();
    params.set(UpdateParams.COMMIT, "true");
    return process(client, collection);
  }

  private interface ReqSupplier<T extends LBSolrClient.Req> {
    T get(SolrRequest solrRequest, ObjectList<String> servers);
  }

  private <T extends LBSolrClient.Req> Object2ObjectMap<String, T> getRoutes(DocRouter router,
                                                                               DocCollection col, Object2ObjectMap<String,ObjectList<String>> urlMap,
                                                                               ModifiableSolrParams params, String idField,
                                                                               ReqSupplier<T> reqSupplier) {
    if ((documents == null || documents.size() == 0)
        && (deleteById == null || deleteById.size() == 0)) {
      return null;
    }

    Object2ObjectMap<String,T> routes = new Object2ObjectOpenHashMap<>(8, 0.5f);
    if (documents != null) {
      Set<Entry<SolrInputDocument,Map<String,Object>>> entries = documents.entrySet();
      for (Entry<SolrInputDocument,Map<String,Object>> entry : entries) {
        SolrInputDocument doc = entry.getKey();
        Object id = doc.getFieldValue(idField);
        if (id == null) {
          return null;
        }
        Slice slice = router.getTargetSlice(id
            .toString(), doc, null, null, col);
        if (slice == null) {
          return null;
        }
        ObjectList<String> urls = urlMap.get(slice.getName());
        if (urls == null) {
          return null;
        }
        String leaderUrl = urls.get(0);
        T request = routes
            .get(leaderUrl);
        if (request == null) {
          UpdateRequest updateRequest = new UpdateRequest();
          updateRequest.setMethod(getMethod());
          updateRequest.setCommitWithin(getCommitWithin());
          updateRequest.setParams(params);
          updateRequest.setPath(getPath());
          updateRequest.setBasicAuthCredentials(getBasicAuthUser(), getBasicAuthPassword());
          updateRequest.setResponseParser(getResponseParser());
          request = reqSupplier.get(updateRequest, urls);
          routes.put(leaderUrl, request);
        }
        UpdateRequest urequest = (UpdateRequest) request.getRequest();
        Map<String,Object> value = entry.getValue();
        Boolean ow = null;
        if (value != null) {
          ow = (Boolean) value.get(OVERWRITE);
        }
        if (ow != null) {
          urequest.add(doc, ow);
        } else {
          urequest.add(doc);
        }
      }
    }

    // Route the deleteById's

    if (deleteById != null) {

      Iterator<Map.Entry<String,Map<String,Object>>> entries = deleteById.entrySet()
          .iterator();
      while (entries.hasNext()) {

        Map.Entry<String,Map<String,Object>> entry = entries.next();

        String deleteId = entry.getKey();
        Map<String,Object> map = entry.getValue();
        Long version = null;
        if (map != null) {
          version = (Long) map.get(VER);
        }
        Slice slice = router.getTargetSlice(deleteId, null, null, null, col);
        if (slice == null) {
          return null;
        }
        ObjectList<String> urls = urlMap.get(slice.getName());
        if (urls == null) {
          return null;
        }
        String leaderUrl = urls.get(0);
        T request = routes.get(leaderUrl);
        if (request != null) {
          UpdateRequest urequest = (UpdateRequest) request.getRequest();
          urequest.deleteById(deleteId, version);
        } else {
          UpdateRequest urequest = new UpdateRequest();
          urequest.setParams(params);
          urequest.deleteById(deleteId, version);
          urequest.setCommitWithin(getCommitWithin());
          urequest.setBasicAuthCredentials(getBasicAuthUser(), getBasicAuthPassword());
          request = reqSupplier.get(urequest, urls);
          routes.put(leaderUrl, request);
        }
      }
    }

    return routes;
  }
  
  /**
   * @param router to route updates with
   * @param col DocCollection for the updates
   * @param urlMap of the cluster
   * @param params params to use
   * @param idField the id field
   * @return a Map of urls to requests
   */
  public Object2ObjectMap<String, LBSolrClient.Req> getRoutesToCollection(DocRouter router,
                                                             DocCollection col, Object2ObjectMap<String,ObjectList<String>> urlMap,
                                                             ModifiableSolrParams params, String idField) {
    return getRoutes(router, col, urlMap, params, idField, LBSolrClient.Req::new);
  }
  
  /**
   * @param router to route updates with
   * @param col DocCollection for the updates
   * @param urlMap of the cluster
   * @param params params to use
   * @param idField the id field
   * @return a Map of urls to requests
   * @deprecated since 8.0, uses {@link #getRoutesToCollection(DocRouter, DocCollection, Object2ObjectMap, ModifiableSolrParams, String)} instead
   */
  @Deprecated
  public Map<String,LBHttpSolrClient.Req> getRoutes(DocRouter router,
      DocCollection col, Object2ObjectMap<String,ObjectList<String>> urlMap,
      ModifiableSolrParams params, String idField) {
    return getRoutes(router, col, urlMap, params, idField, LBHttpSolrClient.Req::new);
  }
  
  public void setDocIterator(Iterator<SolrInputDocument> docIterator) {
    this.docIterator = docIterator;
  }
  
  public void setDeleteQuery(ObjectList<String> deleteQuery) {
    this.deleteQuery = deleteQuery;
  }
  
  // --------------------------------------------------------------------------
  // --------------------------------------------------------------------------
  
  @Override
  public Collection<ContentStream> getContentStreams() throws IOException {
    return ClientUtils.toContentStreams(getXML(), ClientUtils.TEXT_XML);
  }
  
  public String getXML() throws IOException {
    StringWriter writer = new StringWriter();
    writeXML(writer);
    
    // If action is COMMIT or OPTIMIZE, it is sent with params
    String xml = writer.toString();
    // System.out.println( "SEND:"+xml );
    return (xml.length() > 0) ? xml : null;
  }
  
  private List<Map<SolrInputDocument,Map<String,Object>>> getDocLists(Map<SolrInputDocument,Map<String,Object>> documents) {
    List<Map<SolrInputDocument,Map<String,Object>>> docLists = new ObjectArrayList<>();
    Map<SolrInputDocument,Map<String,Object>> docList = null;
    if (this.documents != null) {
      
      Boolean lastOverwrite = true;
      Integer lastCommitWithin = -1;
      
      Set<Entry<SolrInputDocument,Map<String,Object>>> entries = this.documents
          .entrySet();
      for (Entry<SolrInputDocument,Map<String,Object>> entry : entries) {
        Map<String,Object> map = entry.getValue();
        Boolean overwrite = null;
        Integer commitWithin = null;
        if (map != null) {
          overwrite = (Boolean) entry.getValue().get(OVERWRITE);
          commitWithin = (Integer) entry.getValue().get(COMMIT_WITHIN);
        }
        if (overwrite != lastOverwrite || (commitWithin != null && commitWithin.equals(lastCommitWithin))
            || docLists.size() == 0) {
          docList = new Object2ObjectLinkedOpenHashMap<>(32, .5f);
          docLists.add(docList);
        }
        docList.put(entry.getKey(), entry.getValue());
        lastCommitWithin = commitWithin;
        lastOverwrite = overwrite;
      }
    }
    
    if (docIterator != null) {
      docList = new Object2ObjectLinkedOpenHashMap<>(32, .5f);
      docLists.add(docList);
      while (docIterator.hasNext()) {
        SolrInputDocument doc = docIterator.next();
        if (doc != null) {
          docList.put(doc, null);
        }
      }
      
    }

    return docLists;
  }
  
  /**
   * @since solr 1.4
   */
  public UpdateRequest writeXML(Writer writer) throws IOException {
    List<Map<SolrInputDocument,Map<String,Object>>> getDocLists = getDocLists(documents);
    StringBuilder sb = new StringBuilder(32);
    for (Map<SolrInputDocument,Map<String,Object>> docs : getDocLists) {
      sb.setLength(0);
      if ((docs != null && docs.size() > 0)) {
        Entry<SolrInputDocument,Map<String,Object>> firstDoc = docs.entrySet()
            .iterator().next();
        Map<String,Object> map = firstDoc.getValue();
        Integer cw = null;
        Boolean ow = null;
        if (map != null) {
          cw = (Integer) firstDoc.getValue().get(COMMIT_WITHIN);
          ow = (Boolean) firstDoc.getValue().get(OVERWRITE);
        }
        if (ow == null) ow = true;
        int commitWithin = (cw != null && cw != -1) ? cw : this.commitWithin;
        boolean overwrite = ow;
        if (commitWithin > -1 || overwrite != true) {
          writer.write(sb.append("<add commitWithin=\"").append(commitWithin).append("\" ").append("overwrite=\"").append(overwrite).append("\">").toString());
        } else {
          writer.write("<add>");
        }
        
        Set<Entry<SolrInputDocument,Map<String,Object>>> entries = docs
            .entrySet();
        for (Entry<SolrInputDocument,Map<String,Object>> entry : entries) {
          ClientUtils.writeXML(entry.getKey(), writer);
        }
        
        writer.write("</add>");
      }
    }
    
    // Add the delete commands
    boolean deleteI = deleteById != null && deleteById.size() > 0;
    boolean deleteQ = deleteQuery != null && deleteQuery.size() > 0;
    if (deleteI || deleteQ) {
      if (commitWithin > 0) {
        writer.append("<delete commitWithin=\"").append(String.valueOf(commitWithin)).append("\">");
      } else {
        writer.append("<delete>");
      }
      if (deleteI) {
        for (Map.Entry<String,Map<String,Object>> entry : deleteById.entrySet()) {
          writer.append("<id");
          Map<String,Object> map = entry.getValue();
          if (map != null) {
            Long version = (Long) map.get(VER);
            String route = (String)map.get(_ROUTE_);
            if (version != null) {
              writer.append(" version=\"").append(String.valueOf(version)).append('"');
            }
            
            if (route != null) {
              writer.append(" _route_=\"").append(route).append('"');
            }
          }
          writer.append(">");
          
          XML.escapeCharData(entry.getKey(), writer);
          writer.append("</id>");
        }
      }
      if (deleteQ) {
        for (String q : deleteQuery) {
          writer.append("<query>");
          XML.escapeCharData(q, writer);
          writer.append("</query>");
        }
      }
      writer.append("</delete>");
    }
    return this;
  }
  
  // --------------------------------------------------------------------------
  // --------------------------------------------------------------------------
  
  // --------------------------------------------------------------------------
  //
  // --------------------------------------------------------------------------
  
  public List<SolrInputDocument> getDocuments() {
    if (documents == null) return null;
    List<SolrInputDocument> docs = new ArrayList<>(documents.size());
    docs.addAll(documents.keySet());
    return docs;
  }
  
  public Map<SolrInputDocument,Map<String,Object>> getDocumentsMap() {
    return documents;
  }
  
  public Iterator<SolrInputDocument> getDocIterator() {
    return docIterator;
  }
  
  public List<String> getDeleteById() {
    if (deleteById == null) return null;
    List<String> deletes = new ArrayList<>(deleteById.keySet());
    return deletes;
  }
  
  public Map<String,Map<String,Object>> getDeleteByIdMap() {
    return deleteById;
  }
  
  public ObjectList<String> getDeleteQuery() {
    return deleteQuery;
  }
  
  public boolean isLastDocInBatch() {
    return isLastDocInBatch;
  }
  
  public void lastDocInBatch() {
    isLastDocInBatch = true;
  }

  @Override public String toString() {
    return "UpdateRequest{" + "usev2=" + usev2 + ", useBinaryV2=" + useBinaryV2 + ", params=" + params + ", commitWithin=" + commitWithin + ", documents="
        + (documents == null ? "(null)" : documents.size()) + ", docIterator=" + (docIterator != null) + ", deleteById=" + (deleteById == null ? "(null)" : deleteById.size()) + ", deleteQuery=" + deleteQuery + ", isLastDocInBatch=" + isLastDocInBatch
        + '}';
  }

}
