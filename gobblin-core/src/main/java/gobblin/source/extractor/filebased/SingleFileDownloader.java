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
import java.io.InputStream;
import java.util.Iterator;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.io.IOUtils;

import gobblin.configuration.ConfigurationKeys;


/**
 * A {@link FileDownloader} that downloads a single file and iterates line by line.
 *
 * @param <D> record type in the file
 */
@Slf4j
public class SingleFileDownloader<D> extends FileDownloader<D> {

  public SingleFileDownloader(FileBasedExtractor<?, ?> fileBasedExtractor) {
    super(fileBasedExtractor);
  }

  @SuppressWarnings("unchecked")
  public Iterator<D> downloadFile(String file) throws IOException {

    log.info("Beginning to download file: " + file);

    try {
      InputStream inputStream =
          this.fileBasedExtractor.getCloser().register(this.fileBasedExtractor.getFsHelper().getFileStream(file));
      Iterator<D> fileItr = (Iterator<D>) IOUtils.lineIterator(inputStream, ConfigurationKeys.DEFAULT_CHARSET_ENCODING);
      if (this.fileBasedExtractor.isShouldSkipFirstRecord() && fileItr.hasNext()) {
        fileItr.next();
      }
      return fileItr;
    } catch (FileBasedHelperException e) {
      throw new IOException("Exception while downloading file " + file + " with message " + e.getMessage(), e);
    }
  }
}
