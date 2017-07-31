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
package org.apache.gobblin.source.extractor.filebased;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.opencsv.CSVReader;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;

/**
 * A {@link FileDownloader} that downloads a single file and iterates line by line.
 *
 * @param <D> record type in the file
 */
@Slf4j
public class CsvFileDownloader extends FileDownloader<String[]> {

  public static final String CSV_DOWNLOADER_PREFIX = "source.csv_file.";
  public static final String SKIP_TOP_ROWS_REGEX = CSV_DOWNLOADER_PREFIX + "skip_top_rows_regex";
  public static final String DELIMITER = CSV_DOWNLOADER_PREFIX + "delimiter";

  public CsvFileDownloader(FileBasedExtractor<?, ?> fileBasedExtractor) {
    super(fileBasedExtractor);
  }

  /**
   * Provide iterator via OpenCSV's CSVReader.
   * Provides a way to skip top rows by providing regex.(This is useful when CSV file comes with comments on top rows, but not in fixed size.
   * It also provides validation on schema by matching header names between property's schema and header name in CSV file.
   *
   * {@inheritDoc}
   * @see org.apache.gobblin.source.extractor.filebased.FileDownloader#downloadFile(java.lang.String)
   */
  @SuppressWarnings("unchecked")
  @Override
  public Iterator<String[]> downloadFile(String file) throws IOException {

    log.info("Beginning to download file: " + file);
    final State state = fileBasedExtractor.workUnitState;

    CSVReader reader;
    try {
      if (state.contains(DELIMITER)) {
        String delimiterStr = state.getProp(DELIMITER).trim();
        Preconditions.checkArgument(delimiterStr.length() == 1, "Delimiter should be a character.");

        char delimiter = delimiterStr.charAt(0);
        log.info("Using " + delimiter + " as a delimiter.");

        reader = this.fileBasedExtractor.getCloser().register(
            new CSVReader(new InputStreamReader(
                              this.fileBasedExtractor.getFsHelper().getFileStream(file),
                              ConfigurationKeys.DEFAULT_CHARSET_ENCODING), delimiter));
      } else {
        reader = this.fileBasedExtractor.getCloser().register(
            new CSVReader(new InputStreamReader(
                              this.fileBasedExtractor.getFsHelper().getFileStream(file),
                              ConfigurationKeys.DEFAULT_CHARSET_ENCODING)));
      }

    } catch (FileBasedHelperException e) {
      throw new IOException(e);
    }

    PeekingIterator<String[]> iterator = Iterators.peekingIterator(reader.iterator());

    if (state.contains(SKIP_TOP_ROWS_REGEX)) {
      String regex = state.getProp(SKIP_TOP_ROWS_REGEX);
      log.info("Trying to skip with regex: " + regex);
      while (iterator.hasNext()) {
        String[] row = iterator.peek();
        if (row.length == 0) {
          break;
        }

        if (!row[0].matches(regex)) {
          break;
        }
        iterator.next();
      }
    }

    if (this.fileBasedExtractor.isShouldSkipFirstRecord() && iterator.hasNext()) {
      log.info("Skipping first record");
      iterator.next();
    }
    return iterator;
  }
}
