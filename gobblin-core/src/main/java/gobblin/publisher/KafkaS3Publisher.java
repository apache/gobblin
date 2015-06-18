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
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.Random;

/**
 * An implementation of {@link BaseS3Publisher} that publishes Kafka data to S3
 *
 * <p>
 *
 * The bucket is specified by the Kafka topic and the key is specified by the date followed by the writer
 * file name.
 *
 * @author akshay@nerdwallet.com
 */
public class KafkaS3Publisher extends BaseS3Publisher {
  public KafkaS3Publisher(State state) {
    super(state);
  }

  @Override
  public void publishData(Collection<? extends WorkUnitState> states) throws IOException {
    Map<String, ArrayList<String>> data =
            BatchKafkaData.getInputStreams(states, this.numBranches, this.fss);
    for (String k : data.keySet()) {
      String s3Bucket = this.getState().getProp(ConfigurationKeys.S3_PUBLISHER_BUCKET);
      String s3Key = getS3Key(k);
      String[] arr = k.split("-");
      int branch = Integer.parseInt(arr[arr.length - 1]);
      this.sendS3Data(branch, new BucketAndKey(s3Bucket, s3Key), data.get(k));
      int i = 0; // append this to s3 keys to keep it unique per fragment
    }
  }

  @Override
  public void publishMetadata(Collection<? extends WorkUnitState> states) throws IOException {
    // do nothing
  }

  /**
   * The S3 key is &lt;prefix&gt;/&lt;topic&gt;/yyyy/mm/dd, where the prefix is a random number between
   * 0 and {@link BaseS3Publisher#s3Partitions} (non inclusive) for better s3 partitioning.
   *
   * @param batchKey The key used by battching which should be in the format &lt;topic&gt;-&lt;branch&gt;. The
   *                 branch is dropped and the topic is used as part of the key.
   * @return the S3 key described above.
   */
  private String getS3Key(String batchKey) {
    String[] arr = batchKey.split("-");
    Object[] arr1 = ArrayUtils.remove(arr, arr.length - 1);
    String topic = StringUtils.join(arr1, "-");
    int prefix = new Random(System.nanoTime()).nextInt(s3Partitions);
    return String.format(
            "%s/%s",
            topic,
            new SimpleDateFormat("yyyy/MM/dd/HH").format(new Date())
    );
  }
}
