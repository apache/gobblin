/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gobblin.runtime.mapreduce;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.runtime.api.JobExecutionResult;
import org.apache.gobblin.runtime.embedded.EmbeddedGobblin;
import org.apache.gobblin.runtime.task.TaskUtils;
import org.apache.gobblin.source.Source;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.source.workunit.WorkUnit;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.testng.Assert;
import org.testng.annotations.Test;


@Slf4j
public class MRTaskFactoryTest {

  @Test
  public void test() throws Exception {

    File inputSuperPath = Files.createTempDir();
    inputSuperPath.deleteOnExit();
    File outputSuperPath = Files.createTempDir();
    outputSuperPath.deleteOnExit();

    File job1Dir = new File(inputSuperPath, "job1");
    Assert.assertTrue(job1Dir.mkdir());
    writeFileWithContent(job1Dir, "file1", "word1 word1 word2");
    writeFileWithContent(job1Dir, "file2", "word2 word2 word2");

    File job2Dir = new File(inputSuperPath, "job2");
    Assert.assertTrue(job2Dir.mkdir());
    writeFileWithContent(job2Dir, "file1", "word1 word2 word2");

    EmbeddedGobblin embeddedGobblin = new EmbeddedGobblin("WordCounter")
        .setConfiguration(ConfigurationKeys.SOURCE_CLASS_KEY, MRWordCountSource.class.getName())
        .setConfiguration(MRWordCountSource.INPUT_DIRECTORIES_KEY, job1Dir.getAbsolutePath() + "," + job2Dir.getAbsolutePath())
        .setConfiguration(MRWordCountSource.OUTPUT_LOCATION, outputSuperPath.getAbsolutePath());

    JobExecutionResult result = embeddedGobblin.run();
    Assert.assertTrue(result.isSuccessful());

    File output1 = new File(new File(outputSuperPath, "job1"), "part-r-00000");
    Assert.assertTrue(output1.exists());
    Map<String, Integer> counts = parseCounts(output1);
    Assert.assertEquals((int) counts.get("word1"), 2);
    Assert.assertEquals((int) counts.get("word2"), 4);

    File output2 = new File(new File(outputSuperPath, "job2"), "part-r-00000");
    Assert.assertTrue(output2.exists());
    counts = parseCounts(output2);
    Assert.assertEquals((int) counts.get("word1"), 1);
    Assert.assertEquals((int) counts.get("word2"), 2);
  }

  private Map<String, Integer> parseCounts(File file) throws IOException {
    Map<String, Integer> counts = Maps.newHashMap();
    for (String line : Files.readLines(file, Charsets.UTF_8)) {
      List<String> split = Splitter.on("\t").splitToList(line);
      counts.put(split.get(0), Integer.parseInt(split.get(1)));
    }
    return counts;
  }

  private void writeFileWithContent(File dir, String fileName, String content) throws IOException {
    File file = new File(dir, fileName);
    Assert.assertTrue(file.createNewFile());
    Files.write(content, file, Charsets.UTF_8);
  }

  public static class MRWordCountSource implements Source<String, String> {

    public static final String INPUT_DIRECTORIES_KEY = "input.directories";
    public static final String OUTPUT_LOCATION = "output.location";

    @Override
    public List<WorkUnit> getWorkunits(SourceState state) {
      List<String> dirs = Splitter.on(",").splitToList(state.getProp(INPUT_DIRECTORIES_KEY));
      String outputBase = state.getProp(OUTPUT_LOCATION);
      List<WorkUnit> workUnits = Lists.newArrayList();

      for (String dir : dirs) {
        try {
          Path input = new Path(dir);
          Path output = new Path(outputBase, input.getName());

          WorkUnit workUnit = new WorkUnit();
          TaskUtils.setTaskFactoryClass(workUnit, MRTaskFactory.class);
          Configuration conf = new Configuration();
          Job job = Job.getInstance(conf, "WordCount_" + input.getName());
          job.setJarByClass(MRTaskFactoryTest.class);
          job.setMapperClass(TokenizerMapper.class);
          job.setCombinerClass(IntSumReducer.class);
          job.setReducerClass(IntSumReducer.class);
          job.setOutputKeyClass(Text.class);
          job.setOutputValueClass(IntWritable.class);
          job.setNumReduceTasks(1);
          FileInputFormat.addInputPath(job, input);
          FileOutputFormat.setOutputPath(job, output);

          MRTask.serializeJobToState(workUnit, job);
          workUnits.add(workUnit);
        } catch (IOException ioe) {
          log.error("Failed to create MR job for " + dir, ioe);
        }
      }

      return workUnits;
    }

    @Override
    public Extractor<String, String> getExtractor(WorkUnitState state) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void shutdown(SourceState state) {
      throw new UnsupportedOperationException();
    }
  }

  // This is taken directly from
  // https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html
  public static class TokenizerMapper
      extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Mapper.Context context
    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }

  // This is taken directly from
  // https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html
  public static class IntSumReducer
      extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
        Context context
    ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

}
