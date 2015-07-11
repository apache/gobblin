/* (c) 2015 NerdWallet All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.source.extractor.extract.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3Object;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.DataRecordException;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.filebased.FileBasedExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;


/**
 * An extractor for an S3 source. Returns each file, line by line, as strings.
 *
 * @author ahollenbach@nerdwallet.com
 */
public class S3Extractor<S, D> extends FileBasedExtractor<S, D> {

  /**
   * Creates a new S3Extractor
   *
   * @param state the state
   * @throws NullPointerException if the state does not contain the property S3_SOURCE_OBJECT_KEY
   */
  public S3Extractor(WorkUnitState state) {
    super(state, new S3FsHelper(state));
  }
}
