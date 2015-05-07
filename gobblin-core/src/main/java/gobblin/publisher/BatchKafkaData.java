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
import java.io.InputStream;
import java.io.SequenceInputStream;
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
  // Cap the Kafka file size to 5 GB
  private static final long MAX_KAFKA_FILE_SIZE = 5L * 1000000000L;

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
  public static Map<String, LinkedList<LengthAndStream>>
  getInputStreams(Collection<? extends WorkUnitState> tasks, int numBranches, List<FileSystem> fss)
  throws IOException {
    Map<String, LinkedList<LengthAndStream>> data = new HashMap<String, LinkedList<LengthAndStream>>();
    for (WorkUnitState task : tasks) {
      String topic = task.getProp(KafkaSource.TOPIC_NAME);
      for (int i=0; i<numBranches; i++) {
        Path writerFile = new Path(
                task.getProp(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FINAL_OUTPUT_PATH, i))
        );
        // We want one key per topic per branch
        String topicForBranch = topic + "-" + i;

        // Get the input stream and content length
        long contentLength = fss.get(i).getFileStatus(writerFile).getLen();
        InputStream is = fss.get(i).open(writerFile);

        // All the batched streams
        LinkedList<LengthAndStream> lasList = data.get(topicForBranch);
        if (lasList == null) {
          lasList = new LinkedList<LengthAndStream>();
          lasList.add(new LengthAndStream(is, contentLength));
          data.put(topicForBranch, lasList);
        } else {
          LengthAndStream las = lasList.getLast();
          if (!las.appendStream(is, contentLength)) { // stream is too big, add a new one to our list
            lasList.addLast(new LengthAndStream(is, contentLength));
          }
        }
      }
    }
    return data;
  }

  public static class LengthAndStream {
    private long length;
    private InputStream stream;

    public LengthAndStream(InputStream stream, long length) {
      this.length = length;
      this.stream = stream;
    }

    public InputStream getStream() {
      return stream;
    }

    public long getLength() {
      return length;
    }

    public boolean appendStream(InputStream is, long length) {
      if (this.length + length >= MAX_KAFKA_FILE_SIZE) {
        return false; // stream has too much data
      }
      // Chain input streams and update length
      this.stream = new SequenceInputStream(stream, is);
      this.length += length;
      return true;
    }
  }
}
