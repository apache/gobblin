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
import gobblin.source.extractor.utils.InputStreamCSVReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;

/**
 * An extractor for an S3 source. Grabs each line from a source and returns the
 * line parsed as a CSV (using the delimiter set in {@link ConfigurationKeys#CONVERTER_STRING_SPLITTER_DELIMITER}.
 *
 * @author ahollenbach@nerdwallet.com
 */
public class S3CSVExtractor implements Extractor<Class<String>, ArrayList<String>> {

  private static final Logger LOG = LoggerFactory.getLogger(S3CSVExtractor.class);

  protected final WorkUnitState workUnitState;

  protected BufferedReader br;
  protected InputStreamCSVReader csvReader;

  public S3CSVExtractor(WorkUnitState state) {
    this.workUnitState = state;

    AmazonS3 s3Client = new AmazonS3Client();

    // Fetch our object from S3 and build an input stream to read from for each record
    S3Object obj = s3Client.getObject(state.getProp(ConfigurationKeys.S3_SOURCE_BUCKET), state.getProp("OBJECT_KEY"));
    br = new BufferedReader(new InputStreamReader(obj.getObjectContent()));

    csvReader = new InputStreamCSVReader(br, state.getProp(ConfigurationKeys.CONVERTER_STRING_SPLITTER_DELIMITER).charAt(0));
  }

  @Override
  public Class<String> getSchema() throws IOException {
    return String.class;
  }

  @Override
  public ArrayList<String> readRecord(@Deprecated ArrayList<String> reuse) throws DataRecordException, IOException {
    ArrayList<String> record = csvReader.nextRecord();
    LOG.info(record.toString());
    return record;
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
    br.close();
  }
}
