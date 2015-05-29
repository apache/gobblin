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
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.extract.kafka.KafkaSource;
import gobblin.util.ForkOperatorUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * A class to batch data from Kafka tasks corresponding to the same topic into one
 * input stream which can then be published to one file.
 *
 * @author akshay@nerdwallet.com
 */
class BatchKafkaData {
  // Cap the Kafka file size to .5 GB
  private static final long MAX_KAFKA_FILE_SIZE = 500L * 1000000L;

  /**
   * Batches all Kafka for one partition for each branch and returns a mapping from
   * &lt;topic&gt;-&lt;branch&gt; to a list of streams. Each stream in the list is a
   * concatenation of tasks with the same topic in that branch. However, if one stream
   * exceeds {@link BatchKafkaData#MAX_KAFKA_FILE_SIZE}, a new such object is appened
   * to the list. Each element of the list corresponds to one file to be published.
   *
   * @param tasks a collection of all tasks completed in this job
   * @param numBranches number of branches in this job
   * @param fss one file system instance per branch
   * @return a mapping from &lt;topic&gt;-&lt;branch&gt; to a list of streams
   * @throws IOException
   */
  public static Map<String, ArrayList<String>>
  getInputStreams(Collection<? extends WorkUnitState> tasks, int numBranches, List<FileSystem> fss)
  throws IOException {
    Map<String, ArrayList<String>> data = new HashMap<String, ArrayList<String>>();
    for (WorkUnitState task : tasks) {
      String topic = task.getProp(KafkaSource.TOPIC_NAME);
      for (int i=0; i<numBranches; i++) {
        Path writerFile = new Path(
                task.getProp(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FINAL_OUTPUT_PATH, i))
        );
        // We want one key per topic per branch
        String topicForBranch = topic + "-" + i;

        // All the batched streams
        ArrayList<String> fnames = data.get(topicForBranch);
        if (fnames == null) {
          fnames = new ArrayList<String>();
          fnames.add(writerFile.toString());
          data.put(topicForBranch, fnames);
        } else {
          fnames.add(fnames.size() - 1, writerFile.toString());
        }
      }
    }
    return data;
  }
}
