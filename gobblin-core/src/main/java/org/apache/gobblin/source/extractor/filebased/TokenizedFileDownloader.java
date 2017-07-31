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
import java.io.InputStream;
import java.util.Iterator;
import java.util.Scanner;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.apache.gobblin.configuration.ConfigurationKeys;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;


/**
 * Extends {@link FileBasedExtractor<String>} which generates {@link Iterator<String>} using {@link #token} when {@link #downloadFile(String)}.
 */
@Slf4j
public class TokenizedFileDownloader extends FileDownloader<String> {
  public static final String DEFAULT_TOKEN = "\n";

  @Setter
  private String token;

  @Setter
  private String charset;

  public TokenizedFileDownloader(FileBasedExtractor<?, ?> fileBasedExtractor) {
    this(fileBasedExtractor, DEFAULT_TOKEN, ConfigurationKeys.DEFAULT_CHARSET_ENCODING.name());
  }

  public TokenizedFileDownloader(FileBasedExtractor<?, ?> fileBasedExtractor, String token, String charset) {
    super(fileBasedExtractor);
    this.token = token;
    this.charset = charset;
  }

  @Override
  public Iterator<String> downloadFile(String filePath)
      throws IOException {
    Preconditions.checkArgument(this.token != null);
    try {
      log.info("downloading file: " + filePath);
      InputStream inputStream =
          this.fileBasedExtractor.getCloser().register(this.fileBasedExtractor.getFsHelper().getFileStream(filePath));
      return new RecordIterator(inputStream, this.token, this.charset);
    } catch (FileBasedHelperException e) {
      throw new IOException("Exception when trying to download file " + filePath, e);
    }
  }

  @VisibleForTesting
  protected static class RecordIterator implements Iterator<String> {
    Scanner scanner;

    public RecordIterator(InputStream inputStream, String delimiter, String charSet) {
      this.scanner = new Scanner(inputStream, charSet).useDelimiter(delimiter);
    }

    @Override
    public boolean hasNext() {
      boolean hasNextRecord = this.scanner.hasNext();
      if (!hasNextRecord) {
        this.scanner.close();
      }
      return hasNextRecord;
    }

    @Override
    public String next() {
      return this.hasNext() ? this.scanner.next() : null;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Remove is not supported.");
    }
  }
}
