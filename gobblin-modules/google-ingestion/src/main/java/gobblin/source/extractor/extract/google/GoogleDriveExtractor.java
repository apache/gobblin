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
package gobblin.source.extractor.extract.google;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.filebased.FileBasedExtractor;
import gobblin.source.extractor.filebased.FileBasedHelper;

/**
 * Extractor for files in Google drive.
 */
public class GoogleDriveExtractor<S, D> extends FileBasedExtractor<S, D> {
  private static final Logger LOG = LoggerFactory.getLogger(GoogleDriveExtractor.class);

  public GoogleDriveExtractor(WorkUnitState workUnitState, FileBasedHelper fsHelper) {
    super(workUnitState, fsHelper);
  }
}
