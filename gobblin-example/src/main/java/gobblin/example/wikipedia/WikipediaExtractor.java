/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.example.wikipedia;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.message.BasicNameValuePair;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.DataRecordException;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.extract.LongWatermark;


/**
 * An implementation of {@link Extractor} for the Wikipedia example.
 *
 * <p>
 *   This extractor uses the MediaWiki web API to retrieve a certain number of latest revisions
 *   for each specified title from Wikipedia. Each revision is returned as a JSON document.
 * </p>
 *
 * @author Ziyang Liu
 */
public class WikipediaExtractor implements Extractor<String, JsonElement> {

  private static final Logger LOG = LoggerFactory.getLogger(WikipediaExtractor.class);
  private static final DateTimeFormatter WIKIPEDIA_TIMESTAMP_FORMAT = DateTimeFormat.forPattern("YYYYMMddHHmmss");

  public static final String MAX_REVISION_PER_PAGE = "gobblin.wikipediaSource.maxRevisionsPerPage";
  public static final int DEFAULT_MAX_REVISIONS_PER_PAGE = -1;

  public static final String SOURCE_PAGE_TITLES = "source.page.titles";
  public static final String BOOTSTRAP_PERIOD = "wikipedia.source.bootstrap.lookback";
  public static final String DEFAULT_BOOTSTRAP_PERIOD = "P2D";
  public static final String WIKIPEDIA_API_ROOTURL = "wikipedia.api.rooturl";
  public static final String WIKIPEDIA_AVRO_SCHEMA = "wikipedia.avro.schema";

  private static final String JSON_MEMBER_QUERY = "query";
  private static final String JSON_MEMBER_PAGES = "pages";
  private static final String JSON_MEMBER_REVISIONS = "revisions";
  private static final String JSON_MEMBER_PAGEID = "pageid";
  private static final String JSON_MEMBER_TITLE = "title";


  private static final Gson GSON = new Gson();

  private final WikiResponseReader reader;
  private final String rootUrl;
  private final String schema;
  private final String requestedTitle;
  private final int batchSize;
  private final long lastRevisionId;
  private Queue<JsonElement> currentBatch;
  private final ImmutableMap<String, String> baseQuery;
  private final WorkUnitState workUnitState;
  private final int maxRevisionsPulled;

  private class WikiResponseReader implements Iterator<JsonElement> {

    private long lastPulledRevision;
    private long revisionsPulled = 0;

    public WikiResponseReader(long latestPulledRevision) {
      this.lastPulledRevision = latestPulledRevision;
    }

    @Override
    public boolean hasNext() {

      if (WikipediaExtractor.this.maxRevisionsPulled > -1
          && this.revisionsPulled >= WikipediaExtractor.this.maxRevisionsPulled) {
        WikipediaExtractor.this.workUnitState.setActualHighWatermark(new LongWatermark(this.lastPulledRevision));
        LOG.info("Pulled max number of records {}, final revision pulled {}.", this.revisionsPulled,
            this.lastPulledRevision);
        return false;
      }

      if (!WikipediaExtractor.this.currentBatch.isEmpty()) {
        return true;
      } else {

        /*
         * Retrieve revisions for the next title. Repeat until we find a title that has at least one revision,
         * otherwise return false
         */
        if (this.lastPulledRevision >= WikipediaExtractor.this.lastRevisionId) {
          return false;
        }

        try {
          WikipediaExtractor.this.currentBatch = retrievePageRevisions(ImmutableMap.<String, String>builder()
              .putAll(WikipediaExtractor.this.baseQuery)
              .put("rvprop", "ids|timestamp|user|userid|size")
              .put("titles", WikipediaExtractor.this.requestedTitle)
              .put("rvlimit", Integer.toString(WikipediaExtractor.this.batchSize + 1))
              .put("rvstartid", Long.toString(this.lastPulledRevision))
              .put("rvendid", Long.toString(WikipediaExtractor.this.lastRevisionId))
              .put("rvdir", "newer")
              .build());
          // discard the first one (we've already pulled it)
          WikipediaExtractor.this.currentBatch.poll();
        } catch (URISyntaxException | IOException use) {
          LOG.error("Could not retrieve more revisions.", use);
          return false;
        }

        return !WikipediaExtractor.this.currentBatch.isEmpty();
      }
    }

    @Override
    public JsonElement next() {
      if (!hasNext()) {
        return null;
      }
      JsonElement element = WikipediaExtractor.this.currentBatch.poll();
      this.lastPulledRevision = parseRevision(element);
      this.revisionsPulled++;
      return element;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  public WikipediaExtractor(WorkUnitState workUnitState) throws IOException {

    this.workUnitState = workUnitState;
    this.rootUrl = readProp(WIKIPEDIA_API_ROOTURL, workUnitState);
    this.schema = readProp(WIKIPEDIA_AVRO_SCHEMA, workUnitState);

    this.batchSize = 5;
    this.requestedTitle = workUnitState.getProp(ConfigurationKeys.DATASET_URN_KEY);

    this.baseQuery =
        ImmutableMap.<String, String>builder().put("format", "json").put("action","query").put("prop","revisions").build();

    try {
      Queue<JsonElement> lastRevision = retrievePageRevisions(ImmutableMap.<String, String>builder().putAll(this.baseQuery)
          .put("rvprop","ids").put("titles",this.requestedTitle).put("rvlimit","1").build());
      this.lastRevisionId = lastRevision.isEmpty() ? -1 : parseRevision(lastRevision.poll());
    } catch (URISyntaxException use) {
      throw new IOException(use);
    }

    long baseRevision = workUnitState.getWorkunit().getLowWatermark(LongWatermark.class, new Gson()).getValue();
    if (baseRevision < 0) {
      try {
        baseRevision = createLowWatermarkForBootstrap(workUnitState);
      } catch (IOException ioe) {
        baseRevision = this.lastRevisionId;
      }
    }
    this.reader = new WikiResponseReader(baseRevision);

    workUnitState.setActualHighWatermark(new LongWatermark(this.lastRevisionId));
    this.currentBatch = new LinkedList<>();

    LOG.info(String.format("Will pull revisions %s to %s for page %s.",this.reader.lastPulledRevision,
        this.lastRevisionId, this.requestedTitle));

    this.maxRevisionsPulled = workUnitState.getPropAsInt(MAX_REVISION_PER_PAGE, DEFAULT_MAX_REVISIONS_PER_PAGE);
  }

  private long parseRevision(JsonElement element) {
    return element.getAsJsonObject().get("revid").getAsLong();
  }

  private long createLowWatermarkForBootstrap(WorkUnitState state) throws IOException {
    String bootstrapPeriodString = state.getProp(BOOTSTRAP_PERIOD, DEFAULT_BOOTSTRAP_PERIOD);
    Period period = Period.parse(bootstrapPeriodString);
    DateTime startTime = DateTime.now().minus(period);

    try {
      Queue<JsonElement> firstRevision = retrievePageRevisions(ImmutableMap.<String, String>builder().putAll(this.baseQuery)
          .put("rvprop", "ids")
          .put("titles", this.requestedTitle)
          .put("rvlimit", "1")
          .put("rvstart", WIKIPEDIA_TIMESTAMP_FORMAT.print(startTime))
          .put("rvdir", "newer")
          .build());
      if (firstRevision.isEmpty()) {
        throw new IOException("Could not retrieve oldest revision, returned empty revisions list.");
      }
      return parseRevision(firstRevision.poll());
    } catch (URISyntaxException use) {
      throw new IOException(use);
    }

  }

  private String readProp(String key, WorkUnitState workUnitState) {
    String value = workUnitState.getWorkunit().getProp(key);
    if (StringUtils.isBlank(value)) {
      value = workUnitState.getProp(key);
    }
    if (StringUtils.isBlank(value)) {
      value = workUnitState.getJobState().getProp(key);
    }

    return value;
  }

  private boolean isPropPresent(String key, WorkUnitState workUnitState) {
    return workUnitState.contains(key)
        || workUnitState.getWorkunit().contains(key)
        || workUnitState.getJobState().contains(key);
  }

  private JsonElement performHttpQuery(String rootUrl, Map<String, String> query) throws URISyntaxException, IOException {

    List<NameValuePair> queryTokens = Lists.newArrayList();
    for (Map.Entry<String, String> entry : query.entrySet()) {
      queryTokens.add(new BasicNameValuePair(entry.getKey(), entry.getValue()));
    }
    String encodedQuery = URLEncodedUtils.format(queryTokens, Charsets.UTF_8);

    URL actualURL = new URIBuilder(rootUrl).setQuery(encodedQuery).build().toURL();

    Closer closer = Closer.create();
    HttpURLConnection conn = null;
    StringBuilder sb = new StringBuilder();
    try {
      conn = getHttpConnection(actualURL);
      conn.connect();
      BufferedReader br = closer.register(
          new BufferedReader(new InputStreamReader(conn.getInputStream(), ConfigurationKeys.DEFAULT_CHARSET_ENCODING)));
      String line;
      while ((line = br.readLine()) != null) {
        sb.append(line + "\n");
      }
    } catch (Throwable t) {
      throw closer.rethrow(t);
    } finally {
      try {
        closer.close();
      } catch (IOException e) {
        LOG.error("IOException in Closer.close() while performing query " + actualURL);
      }
      if (conn != null) {
        conn.disconnect();
      }
    }

    if (Strings.isNullOrEmpty(sb.toString())) {
      LOG.warn("Received empty response for query: " + actualURL);
      return new JsonObject();
    }

    JsonElement jsonElement = GSON.fromJson(sb.toString(), JsonElement.class);
    return jsonElement;

  }

  private Queue<JsonElement> retrievePageRevisions(Map<String, String> query)
      throws IOException, URISyntaxException {

    Queue<JsonElement> retrievedRevisions = new LinkedList<>();

    JsonElement jsonElement = performHttpQuery(this.rootUrl, query);

    if (jsonElement == null || !jsonElement.isJsonObject()) {
      return retrievedRevisions;
    }

    JsonObject jsonObj = jsonElement.getAsJsonObject();
    if (jsonObj == null || !jsonObj.has(JSON_MEMBER_QUERY)) {
      return retrievedRevisions;
    }

    JsonObject queryObj = jsonObj.getAsJsonObject(JSON_MEMBER_QUERY);
    if (!queryObj.has(JSON_MEMBER_PAGES)) {
      return retrievedRevisions;
    }

    JsonObject pagesObj = queryObj.getAsJsonObject(JSON_MEMBER_PAGES);
    if (pagesObj.entrySet().isEmpty()) {
      return retrievedRevisions;
    }

    JsonObject pageIdObj = pagesObj.getAsJsonObject(pagesObj.entrySet().iterator().next().getKey());
    if (!pageIdObj.has(JSON_MEMBER_REVISIONS)) {
      return retrievedRevisions;
    }

    //retrieve revisions of the current pageTitle
    JsonArray jsonArr = pageIdObj.getAsJsonArray(JSON_MEMBER_REVISIONS);
    for (JsonElement revElement : jsonArr) {
      JsonObject revObj = revElement.getAsJsonObject();

      /*'pageid' and 'title' are associated with the parent object
       * of all revisions. Add them to each individual revision.
       */
      if (pageIdObj.has(JSON_MEMBER_PAGEID)) {
        revObj.add(JSON_MEMBER_PAGEID, pageIdObj.get(JSON_MEMBER_PAGEID));
      }
      if (pageIdObj.has(JSON_MEMBER_TITLE)) {
        revObj.add(JSON_MEMBER_TITLE, pageIdObj.get(JSON_MEMBER_TITLE));
      }
      retrievedRevisions.add(revObj);
    }

    LOG.info(retrievedRevisions.size() + " record(s) retrieved for title " + this.requestedTitle);
    return retrievedRevisions;
  }

  private HttpURLConnection getHttpConnection(URL url) throws IOException {
    Proxy proxy = Proxy.NO_PROXY;
    if (isPropPresent(ConfigurationKeys.SOURCE_CONN_USE_PROXY_URL, this.workUnitState)
        && isPropPresent(ConfigurationKeys.SOURCE_CONN_USE_PROXY_PORT, this.workUnitState)) {
      LOG.info("Use proxy host: " + readProp(ConfigurationKeys.SOURCE_CONN_USE_PROXY_URL, this.workUnitState));
      LOG.info("Use proxy port: " + readProp(ConfigurationKeys.SOURCE_CONN_USE_PROXY_PORT, this.workUnitState));
      InetSocketAddress proxyAddress =
          new InetSocketAddress(readProp(ConfigurationKeys.SOURCE_CONN_USE_PROXY_URL, this.workUnitState),
              Integer.parseInt(readProp(ConfigurationKeys.SOURCE_CONN_USE_PROXY_PORT, this.workUnitState)));
      proxy = new Proxy(Proxy.Type.HTTP, proxyAddress);
    }
    return (HttpURLConnection) url.openConnection(proxy);
  }

  @Override
  public void close() throws IOException {
    // There's nothing to close
  }

  @Override
  public String getSchema() {
    return this.schema;
  }

  @Override
  public JsonElement readRecord(@Deprecated JsonElement reuse) throws DataRecordException, IOException {
    if (this.reader == null) {
      return null;
    }
    if (this.reader.hasNext()) {
      return this.reader.next();
    }
    return null;
  }

  @Override
  public long getExpectedRecordCount() {
    return 0;
  }

  @Override
  public long getHighWatermark() {
    return this.lastRevisionId;
  }

}
