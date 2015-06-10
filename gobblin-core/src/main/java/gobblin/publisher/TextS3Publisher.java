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
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Publishes plaintext files to S3
 *
 * @author ahollenbach@nerdwallet.com
 */
public class TextS3Publisher extends BaseS3Publisher {

  public TextS3Publisher(State state) {
    super(state);
  }

  @Override
  public void publishData(Collection<? extends WorkUnitState> states) throws IOException {
    ArrayList<String> files = new ArrayList<String>();
    for(WorkUnitState state : states) {

      ArrayList<String> fileNames = new ArrayList<String>();
      String s = "";
      for(int i=0; i<this.numBranches; i++) {
        Path writerFile = new Path(state.getProp(ForkOperatorUtils.getPropertyNameForBranch(
                ConfigurationKeys.WRITER_FINAL_OUTPUT_PATH, i)));
        fileNames.add(writerFile.toString());
      }

      String s3Bucket = this.getState().getProp(ConfigurationKeys.S3_BUCKET);
      String s3Key = state.getProp("OBJECT_KEY");
      this.sendS3Data(0, new BucketAndKey(s3Bucket, s3Key), fileNames);
    }
  }

  @Override
  public void publishMetadata(Collection<? extends WorkUnitState> states) throws IOException {

  }
}
