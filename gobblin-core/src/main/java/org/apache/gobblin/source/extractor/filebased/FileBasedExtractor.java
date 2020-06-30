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
import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;
import java.util.List;

import lombok.Getter;

import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.io.Closer;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.instrumented.extractor.InstrumentedExtractor;
import org.apache.gobblin.metrics.Counters;
import org.apache.gobblin.source.extractor.DataRecordException;
import org.apache.gobblin.source.workunit.WorkUnit;


/**
 * Abstract class for file based extractors
 *
 * @author stakiar
 *
 * @param <S>
 *            type of schema
 * @param <D>
 *            type of data record
 */
public class FileBasedExtractor<S, D> extends InstrumentedExtractor<S, D> {

  private static final Logger LOG = LoggerFactory.getLogger(FileBasedExtractor.class);

  protected final WorkUnit workUnit;
  protected final WorkUnitState workUnitState;

  protected final List<String> filesToPull;
  protected final FileDownloader<D> fileDownloader;

  private final int statusCount;
  private long totalRecordCount = 0;

  private Iterator<D> currentFileItr;
  private String currentFile;
  private boolean hasNext = false;

  @Getter
  protected final Closer closer = Closer.create();
  @Getter
  private final boolean shouldSkipFirstRecord;
  @Getter
  protected final SizeAwareFileBasedHelper fsHelper;

  protected enum CounterNames {
    FileBytesRead;
  }

  protected Counters<CounterNames> counters = new Counters<>();

  @SuppressWarnings("unchecked")
  public FileBasedExtractor(WorkUnitState workUnitState, FileBasedHelper fsHelper) {
    super(workUnitState);
    this.workUnitState = workUnitState;
    this.workUnit = workUnitState.getWorkunit();
    this.filesToPull =
        Lists.newArrayList(workUnitState.getPropAsList(ConfigurationKeys.SOURCE_FILEBASED_FILES_TO_PULL, ""));
    this.statusCount = this.workUnit.getPropAsInt(ConfigurationKeys.FILEBASED_REPORT_STATUS_ON_COUNT,
        ConfigurationKeys.DEFAULT_FILEBASED_REPORT_STATUS_ON_COUNT);
    this.shouldSkipFirstRecord = this.workUnitState.getPropAsBoolean(ConfigurationKeys.SOURCE_SKIP_FIRST_RECORD, false);

    if (fsHelper instanceof SizeAwareFileBasedHelper) {
      this.fsHelper = (SizeAwareFileBasedHelper) fsHelper;
    } else {
      this.fsHelper = new SizeAwareFileBasedHelperDecorator(fsHelper);
    }

    try {
      this.fsHelper.connect();
    } catch (FileBasedHelperException e) {
      throw new RuntimeException(e);
    }

    if (workUnitState.contains(ConfigurationKeys.SOURCE_FILEBASED_OPTIONAL_DOWNLOADER_CLASS)) {
      try {
        this.fileDownloader = (FileDownloader<D>) ConstructorUtils.invokeConstructor(
            Class.forName(workUnitState.getProp(ConfigurationKeys.SOURCE_FILEBASED_OPTIONAL_DOWNLOADER_CLASS)), this);
      } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException
          | ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    } else {
      this.fileDownloader = new SingleFileDownloader<>(this);
    }

    this.counters.initialize(getMetricContext(), CounterNames.class, this.getClass());
  }

  /**
   * Initializes a list of files to pull on the first call to the method
   * Iterates through the file and returns a new record upon each call until
   * there are no more records left in the file, then it moves on to the next
   * file
   */
  @Override
  public D readRecordImpl(@Deprecated D reuse) throws DataRecordException, IOException {
    this.totalRecordCount++;

    if (this.statusCount > 0 && this.totalRecordCount % this.statusCount == 0) {
      LOG.info("Total number of records processed so far: " + this.totalRecordCount);
    }

    // If records have been read, check the hasNext value, if not then get the next file to process
    if (this.currentFile != null && this.currentFileItr != null) {
      this.hasNext = this.currentFileItr.hasNext();

      // If the current file is done, move to the next one
      if (!this.hasNext) {
        getNextFileToRead();
      }
    } else {
      // If no records have been read yet, get the first file to process
      getNextFileToRead();
    }

    if (this.hasNext) {
      return this.currentFileItr.next();
    }

    LOG.info("Finished reading records from all files");
    return null;
  }

  /**
   * If a previous file has been read, first close that file. Then search through {@link #filesToPull} to find the first
   * non-empty file.
   */
  private void getNextFileToRead() throws IOException {
    if (this.currentFile != null && this.currentFileItr != null) {
      closeCurrentFile();
      incrementBytesReadCounter();
      // release the reference to allow garbage collection
      this.currentFileItr = null;
    }

    while (!this.hasNext && !this.filesToPull.isEmpty()) {
      this.currentFile = this.filesToPull.remove(0);
      this.currentFileItr = downloadFile(this.currentFile);
      this.hasNext = this.currentFileItr == null ? false : this.currentFileItr.hasNext();
      LOG.info("Will start downloading file: " + this.currentFile);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public S getSchema() {
    return (S) this.workUnit.getProp(ConfigurationKeys.SOURCE_SCHEMA);
  }

  /**
   * Gets a list of commands that will get the expected record count from the
   * source, executes the commands, and then parses the output for the count
   *
   * @return the expected record count
   */
  @Override
  public long getExpectedRecordCount() {
    return -1;
  }

  /**
   * Gets a list of commands that will get the high watermark from the source,
   * executes the commands, and then parses the output for the watermark
   *
   * @return the high watermark
   */
  @Override
  public long getHighWatermark() {
    LOG.info("High Watermark is -1 for file based extractors");
    return -1;
  }

  /**
   * Downloads a file from the source
   *
   * @param file
   *            is the file to download
   * @return an iterator over the file
   * TODO Add support for different file formats besides text e.g. avro iterator, byte iterator, json iterator.
   */
  public Iterator<D> downloadFile(String file) throws IOException {
    return this.fileDownloader.downloadFile(file);
  }

  /**
   * Closes the current file being read.
   */
  public void closeCurrentFile() {
    try {
      this.closer.close();
    } catch (IOException e) {
      if (this.currentFile != null) {
        LOG.error("Failed to close file: " + this.currentFile, e);
      }
    }
  }

  @Override
  public void close() throws IOException {
    try {
      this.fsHelper.close();
    } catch (IOException e) {
      LOG.error("Could not successfully close file system helper due to error: " + e.getMessage(), e);
    }
  }

  private void incrementBytesReadCounter() {
    try {
      this.counters.inc(CounterNames.FileBytesRead, this.fsHelper.getFileSize(this.currentFile));
    } catch (FileBasedHelperException e) {
      LOG.info("Unable to get file size. Will skip increment to bytes counter " + e.getMessage());
      LOG.debug(e.getMessage(), e);
    } catch (UnsupportedOperationException e) {
      LOG.info("Unable to get file size. Will skip increment to bytes counter " + e.getMessage());
      LOG.debug(e.getMessage(), e);
    }
  }
}
