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
package gobblin.source.extractor.filebased;

import java.io.IOException;
import java.util.Iterator;

import gobblin.configuration.ConfigurationKeys;


/**
 * An abstraction for downloading a file in a {@link FileBasedExtractor}. Subclasses are expected to download the file in
 * the {@link #downloadFile(String)} method and return a record iterator. A {@link FileDownloader} can be set in a
 * {@link FileBasedExtractor} by setting {@link ConfigurationKeys#SOURCE_FILEBASED_OPTIONAL_DOWNLOADER_CLASS} in the
 * state.
 *
 * @param <D> record type in the file
 */
public abstract class FileDownloader<D> {

  protected final FileBasedExtractor<?, ?> fileBasedExtractor;

  public FileDownloader(FileBasedExtractor<?, ?> fileBasedExtractor) {
    this.fileBasedExtractor = fileBasedExtractor;
  }

  /**
   * Downloads the file at <code>filePath</code> using the {@link FileBasedExtractor#fsHelper} and returns an
   * {@link Iterator} to the records
   *
   * @param filePath of the file to be downloaded
   * @return An iterator to the records in the file
   */
  public abstract Iterator<D> downloadFile(final String filePath) throws IOException;
}
