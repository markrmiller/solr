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
package org.apache.solr.bench;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

public class DockMakerRamGenTest extends SolrTestCaseJ4 {

    @Test
    public void testBasicCardinality() throws Exception {
      DocMakerRamGen docMaker = new DocMakerRamGen();

      int cardinality = 2;
      docMaker.addField("AlphaCard3", FieldDef.FieldDefBuilder.aFieldDef().withContent(DocMakerRamGen.Content.ALPHEBETIC).withMaxCardinality(cardinality));

      Set<String> values = new HashSet<>();
      for (int i = 0; i < 10; i++) {
        SolrInputDocument doc = docMaker.getDocument();
        SolrInputField field = doc.getField("AlphaCard3");
        values.add(field.getValue().toString());
      }
      assertEquals(values.toString(), cardinality, values.size());

      docMaker = new DocMakerRamGen();
      cardinality = 3;
      docMaker.addField("IntCard2", FieldDef.FieldDefBuilder.aFieldDef().withContent(DocMakerRamGen.Content.INTEGER).withMaxCardinality(cardinality));

      values = new HashSet<>();
      for (int i = 0; i < 10; i++) {
        SolrInputDocument doc = docMaker.getDocument();
        SolrInputField field = doc.getField("IntCard2");
        values.add(field.getValue().toString());
      }
      assertEquals(values.toString(), cardinality, values.size());

      docMaker = new DocMakerRamGen();
      cardinality = 4;
      docMaker.addField("UnicodeCard3", FieldDef.FieldDefBuilder.aFieldDef().withContent(DocMakerRamGen.Content.UNICODE).withMaxCardinality(cardinality));

      values = new HashSet<>();
      for (int i = 0; i < 20; i++) {
        SolrInputDocument doc = docMaker.getDocument();
        SolrInputField field = doc.getField("UnicodeCard3");
        values.add(field.getValue().toString());
      }

      assertEquals(values.toString(), cardinality, values.size());
    }
}
