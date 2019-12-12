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
import com.google.gson.JsonObject;
import com.sforce.async.BulkConnection;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.source.extractor.utils.InputStreamCSVReader;
import org.apache.gobblin.source.extractor.utils.Utils;


/**
 * Result Iterator.
 * Take jobId and 'batchId:resultId,batchId2:resultId2' as input build a result record iterator.
 */
@Slf4j
public class ResultIterator implements Iterator {
  private Iterator<ResultStruct> batchIdResultIdIterator;
  private BulkConnection bulkConnection;
  private InputStreamCSVReader csvReader;
  private List<String> csvHeader;
  private int columnSize;
  private int urlLoadRetryLimit;
  private ResultStruct resultStruct;
  private List<String> currentRecord = null;
  private Boolean isLoadedCurrentRecord = false;
  private int currentFileRowCount = 0;
  private int totalRowCount = 0;

  /**
   *  constructor
   *  need to initiate the reader and currentRecord
   */
  public ResultIterator(BulkConnection bulkConnection, String jobId, String batchIdResultIdString, int urlLoadRetryLimit) {
    this.urlLoadRetryLimit = urlLoadRetryLimit;
    this.bulkConnection = bulkConnection;
    this.batchIdResultIdIterator = this.parsebatchIdResultIdString(jobId, batchIdResultIdString);
    if (this.batchIdResultIdIterator.hasNext()) {
      this.resultStruct = this.batchIdResultIdIterator.next();
      this.csvReader = this.fetchResultsetAsCsvReader(this.resultStruct); // first file reader
    } else {
      throw new RuntimeException("No batch-result id found.");
    }
    this.fulfillCurrentRecord();
    this.csvHeader = this.currentRecord;
    this.columnSize = this.csvHeader.size();
    // after fetching cvs header, clean up status
    this.resetCurrentRecordStatus();
  }

  /**
   * call reader.next and set up currentRecord
   */
  private void fulfillCurrentRecord() {
    if (this.isLoadedCurrentRecord) {
      return; // skip, since CurrentRecord was loaded.
    }
    try {
      this.currentRecord = this.csvReader.nextRecord();
      if (this.currentRecord == null) { // according InputStreamCSVReader, it returns null at the end of the reader.
        log.info("Fetched {} - rows: {}", this.resultStruct, this.currentFileRowCount); // print out log before switch result file
        this.currentFileRowCount = 0; // clean up
        if (this.batchIdResultIdIterator.hasNext()) { // if there is next file, load next file.
          this.resultStruct = this.batchIdResultIdIterator.next();
          this.csvReader = this.fetchResultsetAsCsvReader(resultStruct);
          this.csvReader.nextRecord(); // read and ignore the csv header.
          this.currentRecord = this.csvReader.nextRecord();
        } else {
          log.info("---- Fetched {} rows -----", this.totalRowCount); // print out log when all records were fetched.
        }
      }
      this.isLoadedCurrentRecord = true;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void resetCurrentRecordStatus() {
    this.currentRecord = null;
    this.isLoadedCurrentRecord = false;
  }

  @Override
  public boolean hasNext() {
    this.fulfillCurrentRecord();
    return this.currentRecord != null;
  }

  @Override
  public JsonElement next() {
    this.fulfillCurrentRecord();
    List<String> csvRecord = this.currentRecord;
    this.resetCurrentRecordStatus();
    if (csvRecord == null) {
      throw new NoSuchElementException();
    }
    this.currentFileRowCount++;
    this.totalRowCount++;
    JsonObject jsonObject = Utils.csvToJsonObject(this.csvHeader, csvRecord, this.columnSize);
    return jsonObject;
  }

  /**
   * resultStruct has all data which identify a result set file
   * fetch it and convert to a csvReader
   */
  private InputStreamCSVReader fetchResultsetAsCsvReader(ResultStruct resultStruct) {
    String jobId = resultStruct.jobId;
    String batchId = resultStruct.batchId;
    String resultId = resultStruct.resultId;
    log.info("PK-Chunking workunit: fetching [jobId={}, batchId={}, resultId={}]", jobId, batchId, resultId);
    for (int i = 0; i < this.urlLoadRetryLimit; i++) { // retries
      try {
        InputStream is = this.bulkConnection.getQueryResultStream(jobId, batchId, resultId);
        BufferedReader br = new BufferedReader(new InputStreamReader(is, ConfigurationKeys.DEFAULT_CHARSET_ENCODING));
        return new InputStreamCSVReader(br);
      } catch (Exception e) { // skip, for retry
      }
    }
    // tried fetchRetryLimit times, always getting exception
    throw new RuntimeException("Tried " + this.urlLoadRetryLimit + " times, but couldn't fetch data.");
  }

  /**
   * input string format is "batchId:resultId,batchId2:resultId2"
   * parse it to iterator
   */
  private Iterator<ResultStruct> parsebatchIdResultIdString(String jobId, String batchIdResultIdString) {
    return Arrays.stream(batchIdResultIdString.split(",")).map(x -> x.split(":")).map(x -> new ResultStruct(jobId, x[0], x[1])).iterator();
  }

  @Data
  static class ResultStruct {
    private final String jobId;
    private final String batchId;
    private final String resultId;
    public String toString() {
      return String.format("[jobId=%s, batchId=%s, resultId=%s]", jobId, batchId, resultId);
    }
  }

}
