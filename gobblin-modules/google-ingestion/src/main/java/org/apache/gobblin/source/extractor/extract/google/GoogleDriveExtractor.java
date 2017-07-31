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
package org.apache.gobblin.source.extractor.extract.google;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.extractor.filebased.FileBasedExtractor;
import org.apache.gobblin.source.extractor.filebased.FileBasedHelper;

/**
 * Extractor for files in Google drive.
 */
public class GoogleDriveExtractor<S, D> extends FileBasedExtractor<S, D> {
  private static final Logger LOG = LoggerFactory.getLogger(GoogleDriveExtractor.class);

  public GoogleDriveExtractor(WorkUnitState workUnitState, FileBasedHelper fsHelper) {
    super(workUnitState, fsHelper);
  }
}
