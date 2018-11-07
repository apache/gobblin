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

package org.apache.gobblin.zuora;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import javax.net.ssl.HttpsURLConnection;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.extractor.DataRecordException;
import org.apache.gobblin.source.extractor.resultset.RecordSet;
import org.apache.gobblin.source.extractor.resultset.RecordSetList;
import org.apache.gobblin.source.extractor.utils.InputStreamCSVReader;
import org.apache.gobblin.source.extractor.utils.Utils;

import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.google.gson.JsonElement;


@Alpha
@Slf4j
public class ZuoraClientFilesStreamer {
  private final String outputFormat;
  private final WorkUnitState _workUnitState;
  private final ZuoraClient _client;
  private final int batchSize;
  private final Retryer<Void> _getRetryer;

  private boolean _jobFinished = false;
  private boolean _jobFailed = false;
  private long _totalRecords = 0;

  private BufferedReader _currentReader;
  private int _currentFileIndex = -1;
  private int _skipHeaderIndex = 0; //Indicate whether the header has been skipped for a file.
  private HttpsURLConnection _currentConnection;

  public ZuoraClientFilesStreamer(WorkUnitState workUnitState, ZuoraClient client) {
    _workUnitState = workUnitState;
    _client = client;
    batchSize = workUnitState.getPropAsInt(ConfigurationKeys.SOURCE_QUERYBASED_FETCH_SIZE, 2000);
    outputFormat = _workUnitState.getProp(ZuoraConfigurationKeys.ZUORA_OUTPUT_FORMAT);
    _getRetryer = RetryerBuilder.<Void>newBuilder().retryIfExceptionOfType(IOException.class).withStopStrategy(
        StopStrategies
            .stopAfterAttempt(workUnitState.getPropAsInt(ZuoraConfigurationKeys.ZUORA_API_RETRY_STREAM_FILES_COUNT, 3)))
        .withWaitStrategy(WaitStrategies
            .fixedWait(workUnitState.getPropAsInt(ZuoraConfigurationKeys.ZUORA_API_RETRY_STREAM_FILES_WAIT_TIME, 10000),
                TimeUnit.MILLISECONDS)).build();
  }

  public RecordSet<JsonElement> streamFiles(List<String> fileList, List<String> header)
      throws DataRecordException {
    try {
      if (currentReaderDone()) {
        ++_currentFileIndex;
        closeCurrentSession();
        if (_currentFileIndex >= fileList.size()) {
          log.info("Finished streaming all files.");
          _jobFinished = true;
          return new RecordSetList<>();
        }
        initializeForNewFile(fileList);
      }
      log.info(String
          .format("Streaming file at index %s with id %s ...", _currentFileIndex, fileList.get(_currentFileIndex)));
      InputStreamCSVReader reader = new InputStreamCSVReader(_currentReader);
      if (_skipHeaderIndex == _currentFileIndex) {
        reader.nextRecord(); //skip header
        ++_skipHeaderIndex;
      }

      RecordSetList<JsonElement> rs = new RecordSetList<>();
      List<String> csvRecord;
      int count = 0;
      while ((csvRecord = reader.nextRecord()) != null) {
        rs.add(Utils.csvToJsonObject(header, csvRecord, header.size()));
        ++_totalRecords;
        if (++count >= batchSize) {
          break;
        }
      }
      log.info("Total number of records downloaded: " + _totalRecords);
      return rs;
    } catch (IOException e) {
      try {
        closeCurrentSession();
      } catch (IOException e1) {
        log.error(e1.getMessage());
      }
      _jobFailed = true;
      throw new DataRecordException("Failed to get records from Zuora: " + e.getMessage(), e);
    }
  }

  private void initializeForNewFile(List<String> fileList)
      throws DataRecordException {
    final String fileId = fileList.get(_currentFileIndex);
    log.info(String.format("Start streaming file at index %s with id %s", _currentFileIndex, fileId));

    try {
      _getRetryer.call(new Callable<Void>() {
        @Override
        public Void call()
            throws Exception {
          Pair<HttpsURLConnection, BufferedReader> initialized = createReader(fileId, _workUnitState);
          _currentConnection = initialized.getLeft();
          _currentReader = initialized.getRight();
          return null;
        }
      });
    } catch (Exception e) {
      throw new DataRecordException(
          String.format("Retryer failed: Build connection for streaming failed for file id: %s", fileId), e);
    }
  }

  private Pair<HttpsURLConnection, BufferedReader> createReader(String fileId, WorkUnitState workUnitState)
      throws IOException {
    HttpsURLConnection connection = ZuoraUtil.getConnection(_client.getEndPoint("file/" + fileId), workUnitState);
    connection.setRequestProperty("Accept", "application/json");
    InputStream stream = connection.getInputStream();
    if (StringUtils.isNotBlank(outputFormat) && outputFormat.equalsIgnoreCase("gzip")) {
      stream = new GZIPInputStream(stream);
    }
    return new ImmutablePair<>(connection, new BufferedReader(new InputStreamReader(stream, "UTF-8")));
  }

  private void closeCurrentSession()
      throws IOException {
    if (_currentConnection != null) {
      _currentConnection.disconnect();
    }
    if (_currentReader != null) {
      _currentReader.close();
    }
  }

  private boolean currentReaderDone()
      throws IOException {
    //_currentReader.ready() will be false when there is nothing in _currentReader to be read
    return _currentReader == null || !_currentReader.ready();
  }

  public boolean isJobFinished() {
    return _jobFinished;
  }

  public boolean isJobFailed() {
    return _jobFailed;
  }
}