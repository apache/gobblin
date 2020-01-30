/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gobblin.salesforce;

import com.google.gson.JsonElement;
import com.sforce.async.BulkConnection;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.source.extractor.utils.InputStreamCSVReader;
import org.apache.gobblin.source.extractor.utils.Utils;

/**
 * Iterator for fetching result file of Bulk API.
 */
@Slf4j
public class BulkResultIterator implements Iterator<JsonElement> {
  private FileIdVO fileIdVO;
  private int retryLimit;
  private BulkConnection conn;
  private InputStreamCSVReader csvReader;
  private List<String> header;
  private int columnSize;
  private int lineCount = 0; // this is different than currentFileRowCount. cvs file has header
  private List<String> preLoadedLine = null;

  public BulkResultIterator(BulkConnection conn, FileIdVO fileIdVO, int retryLimit) {
    log.info("create BulkResultIterator: " + fileIdVO);
    this.conn = conn;
    this.fileIdVO = fileIdVO;
    this.retryLimit = retryLimit;
  }

  /**
   * read first data record from cvsReader and initiate header
   * not supposed to do it in constructor function, for delay creating file stream
   */
  private void initHeader() {
    this.header = this.nextLineWithRetry(); // first line is header
    this.columnSize = this.header.size();
    this.preLoadedLine = this.nextLineWithRetry(); // initialize: buffer one record data
  }

  private List<String> nextLineWithRetry() {
    Exception exception = null;
    for (int i = 0; i < retryLimit; i++) {
      try {
        if (this.csvReader == null) {
          this.csvReader = openAndSeekCsvReader(null);
        }
        List<String> line = this.csvReader.nextRecord();
        this.lineCount++;
        return line;
      } catch (InputStreamCSVReader.CSVParseException e) {
        throw new RuntimeException(e); // don't retry if it is parse error
      } catch (Exception e) { // if it is any other exception, retry may resolve the issue.
        exception = e;
        log.info("***Retrying***: {} - {}", fileIdVO, e.getMessage());
        this.csvReader = openAndSeekCsvReader(e);
      }
    }
    throw new RuntimeException("***Retried***: Failed, tried " + retryLimit + " times - ", exception);
  }

  @Override
  public boolean hasNext() {
    if (this.header == null) {
      initHeader();
    }
    return this.preLoadedLine != null;
  }

  @Override
  public JsonElement next() {
    if (this.header == null) {
      initHeader();
    }
    JsonElement jsonObject = Utils.csvToJsonObject(this.header, this.preLoadedLine, this.columnSize);
    this.preLoadedLine = this.nextLineWithRetry();
    if (this.preLoadedLine == null) {
      log.info("----Record count: [{}] for {}", getRowCount(), fileIdVO);
    }
    return jsonObject;
  }

  private InputStreamCSVReader openAndSeekCsvReader(Exception exceptionRetryFor) {
    String jobId = fileIdVO.getJobId();
    String batchId = fileIdVO.getBatchId();
    String resultId = fileIdVO.getResultId();
    log.info("Fetching [jobId={}, batchId={}, resultId={}]", jobId, batchId, resultId);
    closeCsvReader();
    try {
      InputStream is = conn.getQueryResultStream(jobId, batchId, resultId);
      BufferedReader br = new BufferedReader(new InputStreamReader(is, ConfigurationKeys.DEFAULT_CHARSET_ENCODING));
      csvReader = new InputStreamCSVReader(br);
      List<String> lastSkippedLine = null;
      for (int j = 0; j < lineCount; j++) {
        lastSkippedLine = csvReader.nextRecord(); // skip these records
      }
      if ((lastSkippedLine == null && preLoadedLine != null) || (lastSkippedLine != null && !lastSkippedLine.equals(preLoadedLine))) {
        // check if last skipped line is same as the line before error
        throw new RuntimeException("Failed to verify last skipped line - retrying for =>", exceptionRetryFor);
      }
      return csvReader;
    } catch (Exception e) { // failed to open reader and skip lineCount lines
      exceptionRetryFor = exceptionRetryFor != null? exceptionRetryFor : e;
      throw new RuntimeException("Failed to [" + e.getMessage() + "] - retrying for => " , exceptionRetryFor);
    }
  }

  private int getRowCount() {
    // first line is header, last line is `null`,
    // because cvsReader doesn't have hasNext to check end of the stream, we will get null as last line
    return lineCount - 2;
  }

  private void closeCsvReader() {
    if (this.csvReader != null) {
      try {
        this.csvReader.close();
      } catch (IOException e) {
        // ignore the exception
      }
    }
  }
}
