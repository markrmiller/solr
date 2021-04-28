package org.apache.solr.common.util.metrics;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.graphite.GraphiteSender;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.common.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Metrics {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static MetricRegistry MARKS_METRICS = new MetricRegistry();
}
