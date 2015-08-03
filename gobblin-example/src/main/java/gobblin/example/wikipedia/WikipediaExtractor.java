/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
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
import java.net.URL;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;
import com.google.common.io.Closer;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.DataRecordException;
import gobblin.source.extractor.Extractor;
import gobblin.source.workunit.WorkUnit;


/**
 * An implementation of {@link Extractor} for the Wikipedia example.
 *
 * <p>
 *   This extractor uses the MediaWiki web API to retrieve a certain number of latest revisions
 *   for each specified title from Wikipedia. Each revision is returned as a JSON document.
 * </p>
 *
 * @author ziliu
 */
public class WikipediaExtractor implements Extractor<String, JsonElement>{

  private static final Logger LOG = LoggerFactory.getLogger(WikipediaExtractor.class);

  private static final String SOURCE_PAGE_TITLES = "source.page.titles";
  private static final String SOURCE_REVISIONS_CNT = "source.revisions.cnt";
  private static final String WIKIPEDIA_API_ROOTURL = "wikipedia.api.rooturl";
  private static final String WIKIPEDIA_AVRO_SCHEMA = "wikipedia.avro.schema";

  private static final String JSON_MEMBER_QUERY = "query";
  private static final String JSON_MEMBER_PAGES = "pages";
  private static final String JSON_MEMBER_REVISIONS = "revisions";
  private static final String JSON_MEMBER_PAGEID = "pageid";
  private static final String JSON_MEMBER_TITLE = "title";

  private static final Splitter SPLITTER = Splitter.on(",").omitEmptyStrings().trimResults();

  private static final Gson GSON = new Gson();

  private final WorkUnit workUnit;
  private final WikiResponseReader reader;
  private final int revisionsCnt;
  private final String rootUrl;
  private final String schema;
  private final Queue<String> requestedTitles;
  private final int numRequestedTitles;
  private Queue<JsonElement> recordsOfCurrentTitle;

  private class WikiResponseReader implements Iterator<JsonElement> {

    @Override
    public boolean hasNext() {
      if (!recordsOfCurrentTitle.isEmpty()) {
        return true;
      } else if (requestedTitles.isEmpty()) {
        return false;
      } else {

        /*
         * Retrieve revisions for the next title. Repeat until we find a title that has at least one revision,
         * otherwise return false
         */
        while (!requestedTitles.isEmpty()) {
          String currentTitle = requestedTitles.poll();
          try {
            recordsOfCurrentTitle = retrievePageRevisions(currentTitle);
          } catch (IOException e) {
            LOG.error("IOException while retrieving revisions for title '" + currentTitle + "'");
          }
          if (!recordsOfCurrentTitle.isEmpty()) {
            return true;
          }
        }
        return false;
      }
    }

    @Override
    public JsonElement next() {
      if (!hasNext()) {
        return null;
      }
      return WikipediaExtractor.this.recordsOfCurrentTitle.poll();
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  public WikipediaExtractor(WorkUnitState workUnitState) throws IOException {
    this.workUnit = workUnitState.getWorkunit();
    this.rootUrl = this.workUnit.getProp(WIKIPEDIA_API_ROOTURL);
    this.schema = this.workUnit.getProp(WIKIPEDIA_AVRO_SCHEMA);
    this.requestedTitles = new LinkedList<String>(SPLITTER.splitToList(this.workUnit.getProp(SOURCE_PAGE_TITLES)));
    this.revisionsCnt = Integer.parseInt(this.workUnit.getProp(SOURCE_REVISIONS_CNT));
    this.numRequestedTitles = this.requestedTitles.size();

    if (this.requestedTitles.isEmpty()) {
      this.recordsOfCurrentTitle = new LinkedList<JsonElement>();
    } else {
      String firstTitle = this.requestedTitles.poll();
      this.recordsOfCurrentTitle = retrievePageRevisions(firstTitle);
    }

    this.reader = new WikiResponseReader();
  }

  private Queue<JsonElement> retrievePageRevisions(String pageTitle) throws IOException {
    Queue<JsonElement> retrievedRevisions = new LinkedList<JsonElement>();

    Closer closer = Closer.create();
    HttpURLConnection conn = null;
    StringBuilder sb = new StringBuilder();
    String urlStr = this.rootUrl + "&titles=" + pageTitle + "&rvlimit=" + this.revisionsCnt;
    try {
      conn = getHttpConnection(urlStr);
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
        LOG.error("IOException in Closer.close() while retrieving revisions for title '" + pageTitle
            + "' from URL '" + urlStr + "'");
      }
      if (conn != null) {
        conn.disconnect();
      }
    }

    JsonElement jsonElement = GSON.fromJson(sb.toString(), JsonElement.class);

    if (!jsonElement.isJsonObject()) {
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

    LOG.info(retrievedRevisions.size() + " record(s) retrieved for title " + pageTitle);
    return retrievedRevisions;
  }

  private HttpURLConnection getHttpConnection(String urlStr) throws IOException {
    URL url = new URL(urlStr);
    Proxy proxy = Proxy.NO_PROXY;
    if (this.workUnit.contains(ConfigurationKeys.SOURCE_CONN_USE_PROXY_URL) &&
        this.workUnit.contains(ConfigurationKeys.SOURCE_CONN_USE_PROXY_PORT)) {
      LOG.info("Use proxy host: " + this.workUnit.getProp(ConfigurationKeys.SOURCE_CONN_USE_PROXY_URL));
      LOG.info("Use proxy port: " + this.workUnit.getProp(ConfigurationKeys.SOURCE_CONN_USE_PROXY_PORT));
      InetSocketAddress proxyAddress =
          new InetSocketAddress(this.workUnit.getProp(ConfigurationKeys.SOURCE_CONN_USE_PROXY_URL),
              Integer.parseInt(this.workUnit.getProp(ConfigurationKeys.SOURCE_CONN_USE_PROXY_PORT)));
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
    return this.numRequestedTitles * this.revisionsCnt;
  }

  @Override
  public long getHighWatermark() {
    return 0;
  }

}
