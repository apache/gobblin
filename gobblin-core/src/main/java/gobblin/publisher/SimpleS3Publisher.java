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

package gobblin.publisher;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.util.ForkOperatorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Publishes files to S3
 *
 * @author ahollenbach@nerdwallet.com
 */
public class SimpleS3Publisher extends BaseS3Publisher {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleS3Publisher.class);

  /**
   * Creates a new SimpleS3Publisher
   *
   * @param state The state
   * @throws NullPointerException if the state does not contain the property S3_OBJECT_KEY
   */
  public SimpleS3Publisher(State state) {
    super(state);
  }

  @Override
  public void publishData(Collection<? extends WorkUnitState> states) throws IOException {
    ArrayList<String> files = new ArrayList<String>();
    for(WorkUnitState state : states) {

      for(int i=0; i<this.numBranches; i++) {
        ArrayList<String> fileNames = new ArrayList<String>();
        String writerFile = state.getProp(ForkOperatorUtils.getPropertyNameForBranch(
                ConfigurationKeys.WRITER_FINAL_OUTPUT_PATH, i));
        fileNames.add(writerFile);

        try {
          String s3Bucket = state.getProp(ConfigurationKeys.S3_PUBLISHER_BUCKET);
          String s3Path = state.getProp(ConfigurationKeys.S3_PUBLISHER_PATH);
          String s3Key = state.getProp("S3_OBJECT_KEY");

          this.sendS3Data(i, new BucketAndKey(s3Bucket, s3Path + "/" + s3Key), fileNames);
        } catch(NullPointerException ex) {
          LOG.error("S3_OBJECT_KEY not set in state.");
        }
      }
    }
  }

  @Override
  public void publishMetadata(Collection<? extends WorkUnitState> states) throws IOException {

  }
}
