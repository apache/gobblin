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

package com.linkedin.uif.example.helloworld;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.source.extractor.DataRecordException;
import com.linkedin.uif.source.extractor.Extractor;

/**
 * An implementation of {@link Extractor} for the HelloWorld Wikipedia example.
 *
 * @author ziliu
 *
 */

public class HelloWorldExtractor implements Extractor<String, JsonElement>{

  private static final Logger LOG = LoggerFactory.getLogger(HelloWorldExtractor.class);

  private static final String SOURCE_PAGE_TITLES = "source.page.titles";
  private static final String SOURCE_REVISIONS_CNT = "source.revisions.cnt";
  private static final String ROOT_URL = "http://en.wikipedia.org/w/api.php?"
      + "format=json&action=query&prop=revisions&"
      + "rvprop=content|timestamp|user|userid|size";

  private static final Splitter SPLITTER = Splitter.on(",").omitEmptyStrings().trimResults();

  private static final Gson GSON = new Gson();

  private static final String AVRO_WIKIPEDIA_SCHEMA = "{\"namespace\": \"example.wikipedia.avro\",\n" +
      " \"type\": \"record\",\n" +
      " \"name\": \"WikipediaArticle\",\n" +
      " \"fields\": [\n" +
      "     {\"name\": \"pageid\", \"type\": [\"double\", \"null\"]},\n" +
      "     {\"name\": \"title\", \"type\": [\"string\", \"null\"]},\n" +
      "     {\"name\": \"user\", \"type\": [\"string\", \"null\"]},\n" +
      "     {\"name\": \"anon\", \"type\": [\"string\", \"null\"]},\n" +
      "     {\"name\": \"userid\",  \"type\": [\"double\", \"null\"]},\n" +
      "     {\"name\": \"timestamp\", \"type\": [\"string\", \"null\"]},\n" +
      "     {\"name\": \"size\",  \"type\": [\"double\", \"null\"]},\n" +
      "     {\"name\": \"contentformat\",  \"type\": [\"string\", \"null\"]},\n" +
      "     {\"name\": \"contentmodel\",  \"type\": [\"string\", \"null\"]},\n" +
      "     {\"name\": \"content\", \"type\": [\"string\", \"null\"]}\n" +
      " ]\n" +
      "}";

  private List<JsonElement> _elements;
  private WikiResponseReader _reader;

  private class WikiResponseReader implements Iterator<JsonElement> {
    private int _recordsRead;

    private WikiResponseReader() {
      this._recordsRead = 0;
    }

    @Override
    public boolean hasNext() {
      return HelloWorldExtractor.this._elements != null
          && this._recordsRead < HelloWorldExtractor.this._elements.size();
    }

    @Override
    public JsonElement next() {
      if (!hasNext()) return null;
      return HelloWorldExtractor.this._elements.get(this._recordsRead++);
    }
  }

  public HelloWorldExtractor(WorkUnitState workUnitState) throws IOException {
    this._elements = new ArrayList<JsonElement>();
    List<String> pageTitles = SPLITTER.splitToList(workUnitState.getWorkunit().getProp(SOURCE_PAGE_TITLES));

    for (String pageTitle : pageTitles) {
      String urlStr = ROOT_URL + "&titles=" + pageTitle
          + "&rvlimit=" + workUnitState.getWorkunit().getProp(SOURCE_REVISIONS_CNT);
      URL url = null;
      HttpURLConnection conn = null;
      url = new URL(urlStr);
      conn = (HttpURLConnection) url.openConnection();

      BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
      StringBuilder sb = new StringBuilder();
      String line;
      while ((line = br.readLine()) != null) {
        sb.append(line);
      }
      br.close();
      conn.disconnect();
      JsonElement jsonElement = GSON.fromJson(sb.toString(), JsonElement.class);
      JsonObject jsonObj = jsonElement.getAsJsonObject();
      JsonObject queryObj = null, pagesObj = null, pageIdObj = null;
      JsonArray jsonArr = null;
      queryObj = jsonObj.getAsJsonObject("query");
      if (queryObj != null) {
        pagesObj = queryObj.getAsJsonObject("pages");
      }
      if (pagesObj != null && pagesObj.entrySet().size() == 1) {
        pageIdObj = pagesObj.getAsJsonObject(pagesObj.entrySet().iterator().next().getKey());
      }
      if (pageIdObj != null) {

        //retrieve revisions of the current pageTitle
        jsonArr = pageIdObj.getAsJsonArray("revisions");
        for (Iterator<JsonElement> it = jsonArr.iterator(); it.hasNext(); ) {
          JsonElement revElement = it.next();
          JsonObject revObj = revElement.getAsJsonObject();

          /*'pageid' and 'title' are associated with the parent object
           * of all revisions. Add them to each individual revision.
           */
          if (pageIdObj.has("pageid"))
            revObj.add("pageid", pageIdObj.get("pageid"));
          if (pageIdObj.has("title"))
            revObj.add("title", pageIdObj.get("title"));
          this._elements.add((JsonElement) revObj);
        }
      }
      if (pageIdObj == null) {
        LOG.error("Page title '" + pageTitle + "' did not return any revision");
      }
    }
    this._reader = new WikiResponseReader();
  }

  @Override
  public void close() throws IOException {
    // There's nothing to close
  }

  @Override
  public String getSchema() {
    return AVRO_WIKIPEDIA_SCHEMA;
  }

  @Override
  public JsonElement readRecord(JsonElement reuse)
      throws DataRecordException, IOException {
    if (this._reader == null) {
        return null;
      }
    if (this._reader.hasNext()) {
        return this._reader.next();
      }
    return null;
  }

  @Override
  public long getExpectedRecordCount() {
    return this._elements == null ? 0 : this._elements.size();
  }

  @Override
  public long getHighWatermark() {
    return 0;
  }

}
