/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
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

import com.google.common.base.Preconditions;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.filebased.FileBasedExtractor;
import gobblin.source.extractor.filebased.FileBasedHelper;

import lombok.extern.slf4j.Slf4j;


/**
 * Extends {@link FileBasedExtractor} and uses {@link TokenizedFileDownloader}.
 */
public class TokenBasedFileInputStreamExtractor extends FileBasedExtractor<String, String> {

  public static final String TOKEN =
      "gobblin.extractor." + TokenBasedFileInputStreamExtractor.class.getSimpleName() + ".token";

  public static final String CHARSET =
      "gobblin.extractor." + TokenBasedFileInputStreamExtractor.class.getSimpleName() + ".charSet";

  private final String token;
  private final String charSet;

  public TokenBasedFileInputStreamExtractor(WorkUnitState workUnitState, FileBasedHelper fsHelper) {
    super(workUnitState, fsHelper);
    Preconditions.checkArgument(this.fileDownloader instanceof TokenizedFileDownloader);
    this.token = workUnitState.getProp(TOKEN, TokenizedFileDownloader.DEFAULT_TOKEN);
    this.charSet = workUnitState.getProp(CHARSET, ConfigurationKeys.DEFAULT_CHARSET_ENCODING.name());
    ((TokenizedFileDownloader) fileDownloader).setToken(token);
    ((TokenizedFileDownloader) fileDownloader).setCharset(charSet);
  }
}
