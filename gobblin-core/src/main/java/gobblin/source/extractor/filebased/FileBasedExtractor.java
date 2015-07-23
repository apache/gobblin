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

package gobblin.source.extractor.filebased;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.instrumented.extractor.InstrumentedExtractor;
import gobblin.metrics.Counters;
import gobblin.source.extractor.DataRecordException;
import gobblin.source.workunit.WorkUnit;


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
public abstract class FileBasedExtractor<S, D> extends InstrumentedExtractor<S, D> {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileBasedExtractor.class);

  protected final WorkUnit workUnit;
  protected final WorkUnitState workUnitState;
  protected final SizeAwareFileBasedHelper fsHelper;
  protected final List<String> filesToPull;

  protected final Closer closer = Closer.create();

  private final int statusCount;
  private long totalRecordCount = 0;

  private Iterator<D> currentFileItr;
  private String currentFile;
  private boolean readRecordStart;

  protected enum CounterNames {
    FileBytesRead;
  }

  protected Counters<CounterNames> counters = new Counters<CounterNames>();

  public FileBasedExtractor(WorkUnitState workUnitState, FileBasedHelper fsHelper) {
    super(workUnitState);
    this.workUnitState = workUnitState;
    this.workUnit = workUnitState.getWorkunit();
    this.filesToPull =
        Lists.newArrayList(workUnitState.getPropAsList(ConfigurationKeys.SOURCE_FILEBASED_FILES_TO_PULL, ""));
    this.statusCount =
        this.workUnit.getPropAsInt(ConfigurationKeys.FILEBASED_REPORT_STATUS_ON_COUNT,
            ConfigurationKeys.DEFAULT_FILEBASED_REPORT_STATUS_ON_COUNT);

    if (fsHelper instanceof SizeAwareFileBasedHelper) {
      this.fsHelper = (SizeAwareFileBasedHelper) fsHelper;
    } else {
      this.fsHelper = new SizeAwareFileBasedHelperDecorator(fsHelper);
    }
    try {
      this.fsHelper.connect();
    } catch (FileBasedHelperException e) {
      Throwables.propagate(e);
    }

    counters.initialize(getMetricContext(), CounterNames.class, this.getClass());
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
      LOGGER.info("Total number of records processed so far: " + this.totalRecordCount);
    }

    if (!readRecordStart) {
      LOGGER.info("Starting to read records");
      if (!filesToPull.isEmpty()) {
        currentFile = filesToPull.remove(0);
        currentFileItr = downloadFile(currentFile);
        LOGGER.info("Will start downloading file: " + currentFile);
      } else {
        LOGGER.info("Finished reading records from all files");
        return null;
      }
      readRecordStart = true;
    }

    while ((currentFileItr == null || !currentFileItr.hasNext()) && !filesToPull.isEmpty()) {
      LOGGER.info("Finished downloading file: " + currentFile);
      closeCurrentFile();
      incrementBytesReadCounter();
      currentFile = filesToPull.remove(0);
      currentFileItr = downloadFile(currentFile);
      LOGGER.info("Will start downloading file: " + currentFile);
    }

    if (currentFileItr != null && currentFileItr.hasNext()) {
      return currentFileItr.next();
    } else {
      LOGGER.info("Finished reading records from all files");
      return null;
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
    LOGGER.info("High Watermark is -1 for file based extractors");
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
  @SuppressWarnings("unchecked")
  public Iterator<D> downloadFile(String file) throws IOException {
    LOGGER.info("Beginning to download file: " + file);

    try {
      InputStream inputStream = this.closer.register(this.fsHelper.getFileStream(file));
      Iterator<D> fileItr = (Iterator<D>) IOUtils.lineIterator(inputStream, ConfigurationKeys.DEFAULT_CHARSET_ENCODING);
      if (workUnitState.getPropAsBoolean(ConfigurationKeys.SOURCE_SKIP_FIRST_RECORD, false) && fileItr.hasNext()) {
        fileItr.next();
      }
      return fileItr;
    } catch (FileBasedHelperException e) {
      throw new IOException("Exception while downloading file " + file + " with message " + e.getMessage(), e);
    }
  }

  /**
   * Closes the current file being read.
   */
  public void closeCurrentFile() {
    try {
      this.closer.close();
    } catch (IOException e) {
      if (this.currentFile != null) {
        LOGGER.error("Failed to close file: " + this.currentFile, e);
      }
    }
  }

  @Override
  public void close() {
    try {
      this.fsHelper.close();
    } catch (FileBasedHelperException e) {
      LOGGER.error("Could not successfully close file system helper due to error: " + e.getMessage(), e);
    }
  }

  private void incrementBytesReadCounter() {
    try {
      counters.inc(CounterNames.FileBytesRead, fsHelper.getFileSize(currentFile));
    } catch (FileBasedHelperException e) {
      LOGGER.info("Unable to get file size. Will skip increment to bytes counter " + e.getMessage());
      LOGGER.debug(e.getMessage(), e);
    } catch (UnsupportedOperationException e) {
      LOGGER.info("Unable to get file size. Will skip increment to bytes counter " + e.getMessage());
      LOGGER.debug(e.getMessage(), e);
    }
  }
}
