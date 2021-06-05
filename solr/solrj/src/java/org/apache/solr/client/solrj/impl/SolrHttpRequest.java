package org.apache.solr.client.solrj.impl;

import org.agrona.MutableDirectBuffer;
import org.apache.solr.common.util.ExpandableBuffers;
import org.apache.solr.common.util.SolrInternalHttpClient;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.HttpConversation;
import org.eclipse.jetty.client.HttpRequest;
import org.eclipse.jetty.client.api.Request;

import java.net.URI;

public class SolrHttpRequest extends HttpRequest {
  private final MutableDirectBuffer buffer;

  public SolrHttpRequest(SolrInternalHttpClient client, HttpConversation conversation, String uri, MutableDirectBuffer buffer) {
    super(client, conversation, URI.create(uri));
    this.buffer = buffer;
  }

  public void freeBuffer() {
    if (buffer != null) {
      ExpandableBuffers.getInstance().release(buffer);
    }
  }
}
