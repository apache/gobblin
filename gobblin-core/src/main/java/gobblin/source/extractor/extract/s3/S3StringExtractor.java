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
public class S3StringExtractor implements Extractor<Class<String>, String> {

  private static final Logger LOG = LoggerFactory.getLogger(S3StringExtractor.class);

  protected final WorkUnitState workUnitState;

  protected BufferedReader br;

  /**
   * Creates a new S3StringExtractor
   *
   * @param state the state
   * @throws NullPointerException if the state does not contain the property S3_SOURCE_OBJECT_KEY
   */
  public S3StringExtractor(WorkUnitState state) {
    this.workUnitState = state;

    AmazonS3 s3Client = new AmazonS3Client();

    try {
      String s3Path = state.getProp(ConfigurationKeys.S3_SOURCE_PATH);
      String objectKey = state.getProp("S3_SOURCE_OBJECT_KEY");
      if (objectKey == null) {
        throw new NullPointerException();
      }

      // Fetch our object from S3 and build an input stream to read from for each record
      S3Object obj = s3Client.getObject(state.getProp(ConfigurationKeys.S3_SOURCE_BUCKET), s3Path + "/" + objectKey);
      br = new BufferedReader(new InputStreamReader(obj.getObjectContent()));

    } catch (NullPointerException ex) {
      LOG.error("S3_SOURCE_OBJECT_KEY not set in state.");
    }
  }

  @Override
  public Class<String> getSchema()
      throws IOException {
    return String.class;
  }

  @Override
  public String readRecord(@Deprecated String reuse)
      throws DataRecordException, IOException {
    return br.readLine();
  }

  @Override
  public long getExpectedRecordCount() {
    return 0;
  }

  @Override
  public long getHighWatermark() {
    return 0;
  }

  @Override
  public void close()
      throws IOException {
    br.close();
  }
}
