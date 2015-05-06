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
import org.apache.hadoop.fs.Path;

import java.text.SimpleDateFormat;
import java.util.Date;

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
  protected BucketAndKey getBucketAndKey(WorkUnitState task, int branch) {
    String bucket = task.getProp(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY);
    String key = new SimpleDateFormat("dd/MM/yyyy/").format(new Date()) +
            new Path(task.getProp(ConfigurationKeys.WRITER_FINAL_OUTPUT_PATH)).getName();
    return new BucketAndKey(bucket, key);
  }
}
