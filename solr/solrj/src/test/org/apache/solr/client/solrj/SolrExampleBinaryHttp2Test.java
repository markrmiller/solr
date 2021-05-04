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

package org.apache.solr.client.solrj;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.BinaryRequestWriter;
import org.apache.solr.client.solrj.impl.BinaryResponseParser;
import org.apache.solr.client.solrj.impl.Http2SolrClient;

/**
 * A subclass of SolrExampleTests that explicitly uses the binary
 * codec for communication.
 */
@SolrTestCaseJ4.SuppressSSL(bugUrl = "https://issues.apache.org/jira/browse/SOLR-5776")
public class SolrExampleBinaryHttp2Test extends SolrExampleTests {

  public SolrClient createNewSolrClient(JettySolrRunner jetty)
  {
    try {
      // setup the server...SolrExampleXMLHttp2Test
      String url = jetty.getBaseUrl().toString() + "/collection1";
      Http2SolrClient client = new Http2SolrClient.Builder(url)
          .connectionTimeout(DEFAULT_CONNECTION_TIMEOUT)
          .build();

      // where the magic happens
      client.setParser(new BinaryResponseParser());
      client.setRequestWriter(new BinaryRequestWriter());

      return client;
    }
    catch( Exception ex ) {
      throw new RuntimeException( ex );
    }
  }
}
