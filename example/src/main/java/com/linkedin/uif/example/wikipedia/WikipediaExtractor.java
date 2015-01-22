/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.uif.example.wikipedia;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;
import com.google.common.io.Closer;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.source.extractor.DataRecordException;
import com.linkedin.uif.source.extractor.Extractor;

/**
 * An implementation of {@link Extractor} for the Wikipedia example.
 *
 * @author ziliu
 */
public class WikipediaExtractor implements Extractor<String, JsonElement>{

  private static final Logger LOG = LoggerFactory.getLogger(WikipediaExtractor.class);

  private static final String SOURCE_PAGE_TITLES = "source.page.titles";
  private static final String SOURCE_REVISIONS_CNT = "source.revisions.cnt";
  private static final String WIKIPEDIA_API_ROOTURL = "wikipedia.api.rooturl";
  private static final String WIKIPEDIA_AVRO_SCHEMA = "wikipedia.avro.schema";

  private static final Splitter SPLITTER = Splitter.on(",").omitEmptyStrings().trimResults();

  private static final Gson GSON = new Gson();

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
            recordsOfCurrentTitle =
                retrievePageRevisions(currentTitle);
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
  }

  public WikipediaExtractor(WorkUnitState workUnitState) throws IOException {
    rootUrl = workUnitState.getWorkunit().getProp(WIKIPEDIA_API_ROOTURL);
    schema = workUnitState.getWorkunit().getProp(WIKIPEDIA_AVRO_SCHEMA);
    requestedTitles = new LinkedList<String>(SPLITTER.splitToList(workUnitState.getWorkunit().getProp(SOURCE_PAGE_TITLES)));
    revisionsCnt = Integer.parseInt(workUnitState.getWorkunit().getProp(SOURCE_REVISIONS_CNT));
    numRequestedTitles = requestedTitles.size();

    if (requestedTitles.isEmpty()) {
      recordsOfCurrentTitle = new LinkedList<JsonElement>();
    } else {
      String firstTitle = requestedTitles.poll();
      recordsOfCurrentTitle = retrievePageRevisions(firstTitle);
    }

    this.reader = new WikiResponseReader();
  }

  private Queue<JsonElement> retrievePageRevisions(String pageTitle) throws IOException {
    Queue<JsonElement> retrievedRevisions = new LinkedList<JsonElement>();

    Closer closer = Closer.create();
    HttpURLConnection conn = null;
    StringBuilder sb = new StringBuilder();
    String urlStr = rootUrl + "&titles=" + pageTitle
        + "&rvlimit=" + revisionsCnt;
    try {
      URL url = new URL(urlStr);
      conn = (HttpURLConnection) url.openConnection();
      BufferedReader br = closer.register(new BufferedReader(new InputStreamReader(conn.getInputStream())));
      String line;
      while ((line = br.readLine()) != null) {
        sb.append(line + "\n");
      }
    } finally {
      try {
        closer.close();
      } catch (IOException e) {
        LOG.error("IOException in Closer.close() while retrieving revisions for title '" + pageTitle + "'");
      }
      if (conn != null) {
        conn.disconnect();
      }
    }

    JsonElement jsonElement = GSON.fromJson(sb.toString(), JsonElement.class);

    JsonObject jsonObj = jsonElement.getAsJsonObject();
    if (jsonObj == null || !jsonObj.has("query")) {
      return retrievedRevisions;
    }

    JsonObject queryObj = jsonObj.getAsJsonObject("query");
    if (!queryObj.has("pages")) {
      return retrievedRevisions;
    }

    JsonObject pagesObj = queryObj.getAsJsonObject("pages");
    if (pagesObj.entrySet().isEmpty()) {
      return retrievedRevisions;
    }

    JsonObject pageIdObj = pagesObj.getAsJsonObject(pagesObj.entrySet().iterator().next().getKey());
    if (!pageIdObj.has("revisions")) {
      return retrievedRevisions;
    }

    //retrieve revisions of the current pageTitle
    JsonArray jsonArr = pageIdObj.getAsJsonArray("revisions");
    for (JsonElement revElement : jsonArr) {
      JsonObject revObj = revElement.getAsJsonObject();

      /*'pageid' and 'title' are associated with the parent object
       * of all revisions. Add them to each individual revision.
       */
      if (pageIdObj.has("pageid")) {
        revObj.add("pageid", pageIdObj.get("pageid"));
      }
      if (pageIdObj.has("title")) {
        revObj.add("title", pageIdObj.get("title"));
      }
      retrievedRevisions.add((JsonElement) revObj);
    }

    LOG.info(retrievedRevisions.size() + " record(s) retrieved for title " + pageTitle);
    return retrievedRevisions;
  }

  @Override
  public void close() throws IOException {
    // There's nothing to close
  }

  @Override
  public String getSchema() {
    return schema;
  }

  @Override
  public JsonElement readRecord(JsonElement reuse)
      throws DataRecordException, IOException {
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
    return numRequestedTitles * this.revisionsCnt;
  }

  @Override
  public long getHighWatermark() {
    return 0;
  }

}
