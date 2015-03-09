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

package gobblin.source.extractor.filebased;

import gobblin.source.extractor.DataRecordException;
import gobblin.source.extractor.Extractor;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
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
 * @param <K>
 *            key type of the command output
 * @param <V>
 *            value type of the command output
 */
public class FileBasedExtractor<S, D> implements Extractor<S, D> {
  private Logger log = LoggerFactory.getLogger(FileBasedExtractor.class);
  private Iterator<D> currentFileItr;
  private String currentFile;
  private boolean readRecordStart;
  private boolean supportsReuse = true;
  private boolean seenFirstRecord = false;

  protected WorkUnit workUnit;
  protected WorkUnitState workUnitState;
  protected FileBasedHelper fsHelper;
  protected List<String> filesToPull;
  protected Map<String, Closeable> fileHandles;
  private long totalRecordCount = 0;

  public FileBasedExtractor(WorkUnitState workUnitState, FileBasedHelper fsHelper) {
    this.workUnitState = workUnitState;
    this.workUnit = workUnitState.getWorkunit();
    this.filesToPull =
        new ArrayList<String>(workUnitState.getPropAsList(ConfigurationKeys.SOURCE_FILEBASED_FILES_TO_PULL, ""));
    this.fileHandles = new HashMap<String, Closeable>();
    this.fsHelper = fsHelper;
    try {
      this.fsHelper.connect();
    } catch (FileBasedHelperException e) {
      Throwables.propagate(e);
    }
  }

  /**
   * Initializes a list of files to pull on the first call to the method
   * Iterates through the file and returns a new record upon each call until
   * there are no more records left in the file, then it moves on to the next
   * file
   */
  @Override
  public D readRecord(D reuse)
      throws DataRecordException, IOException {
    this.totalRecordCount++;
    int statusCount = this.workUnit.getPropAsInt(ConfigurationKeys.FILEBASED_REPORT_STATUS_ON_COUNT,
        ConfigurationKeys.DEFAULT_FILEBASED_REPORT_STATUS_ON_COUNT);

    if (statusCount > 0 && this.totalRecordCount % statusCount == 0) {
      this.log.info("Total number of records processed so far: " + this.totalRecordCount);
    }

    if (!readRecordStart) {
      log.info("Starting to read records");
      if (!filesToPull.isEmpty()) {
        currentFile = filesToPull.remove(0);
        currentFileItr = downloadFile(currentFile);
        seenFirstRecord = false;
        log.info("Will start downloading file: " + currentFile);
      } else {
        log.info("Finished reading records from all files");
        return null;
      }
      readRecordStart = true;
    }

    while (!currentFileItr.hasNext() && !filesToPull.isEmpty()) {
      log.info("Finished downloading file: " + currentFile);
      closeFile(currentFile);
      currentFile = filesToPull.remove(0);
      currentFileItr = downloadFile(currentFile);
      seenFirstRecord = false;
      log.info("Will start downloading file: " + currentFile);
    }

    if (currentFileItr.hasNext()) {
      if (supportsReuse && seenFirstRecord) {
        try {
          return (D) currentFileItr.getClass().getMethod("next", reuse.getClass()).invoke(currentFileItr, reuse);
        } catch (Exception e) {
          e.printStackTrace();
          log.info("Object reuse unsupported, continuing without reuse");
          supportsReuse = false;
        }
      }
      seenFirstRecord = true;
      return (D) currentFileItr.next();
    } else {
      log.info("Finished reading records from all files");
      return null;
    }
  }

  /**
   * Get a list of commands to execute on the source file system, executes the
   * commands, and parses the output for the schema
   *
   * @return the schema
   */
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
    log.info("High Watermark is -1 for file based extractors");
    return -1;
  }

  /**
   * Downloads a file from the source
   *
   * @param f
   *            is the file to download
   * @return an iterator over the file
   * @TODO Add support for different file formats besides text e.g. avro
   *       iterator, byte iterator, json iterator
   */
  @SuppressWarnings("unchecked")
  public Iterator<D> downloadFile(String file)
      throws IOException {
    log.info("Beginning to download file: " + file);

    try {
      InputStream inputStream = this.fsHelper.getFileStream(file);
      Iterator<D> fileItr = (Iterator<D>) IOUtils.lineIterator(inputStream, ConfigurationKeys.DEFAULT_CHARSET_ENCODING);
      fileHandles.put(file, inputStream);
      if (workUnitState.getPropAsBoolean(ConfigurationKeys.SOURCE_SKIP_FIRST_RECORD, false) && fileItr.hasNext()) {
        fileItr.next();
      }
      return fileItr;
    } catch (FileBasedHelperException e) {
      throw new IOException("Exception while downloading file " + file + " with message " + e.getMessage(), e);
    }
  }

  /**
   * Closes a file from the source
   *
   * @param f
   *            is the file to download
   * @return an iterator over the file
   */
  public void closeFile(String file) {
    try {
      this.fileHandles.get(file).close();
    } catch (IOException e) {
      log.error("Could not successfully close file: " + file + " due to error: " + e.getMessage(), e);
    }
  }

  @Override
  public void close() {
    try {
      this.fsHelper.close();
    } catch (FileBasedHelperException e) {
      log.error("Could not successfully close file system helper due to error: " + e.getMessage(), e);
    }
  }
}
