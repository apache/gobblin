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

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.DataRecordException;
import gobblin.source.extractor.Extractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.security.krb5.Config;

import java.io.*;

/**
 * An extractor for an S3 source
 *
 * @author ahollenbach@nerdwallet.com
 */
public class S3TextFileExtractor implements Extractor<String,byte[]> {

  private static final Logger LOG = LoggerFactory.getLogger(S3TextFileExtractor.class);

  protected final WorkUnitState workUnitState;

  protected BufferedReader reader;

  public S3TextFileExtractor(WorkUnitState state) {
    this.workUnitState = state;

    AmazonS3 s3Client = new AmazonS3Client();

    // Fetch our object from S3 and build an input stream to read from for each record
    S3Object obj = s3Client.getObject(state.getProp(ConfigurationKeys.S3_SOURCE_BUCKET), state.getProp("OBJECT_KEY"));
    reader = new BufferedReader(new InputStreamReader(obj.getObjectContent()));
  }

  @Override
  public String getSchema() throws IOException {
    return "BAD";
  }

  @Override
  public byte[] readRecord(@Deprecated byte[] reuse) throws DataRecordException, IOException {
    LOG.info("Reading record");
    // returns next line, or null if done
    // TODO might be slow converting back and forth?
    String line = reader.readLine();
    if (line == null) {
      return null;
    }
    LOG.info(line);
    return line.getBytes();
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
  public void close() throws IOException {
    reader.close();
  }
}
