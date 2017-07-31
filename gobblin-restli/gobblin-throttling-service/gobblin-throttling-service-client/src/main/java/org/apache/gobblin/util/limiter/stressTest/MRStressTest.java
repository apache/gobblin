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

package org.apache.gobblin.util.limiter.stressTest;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.joda.time.DateTime;
import org.slf4j.Logger;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.broker.BrokerConfigurationKeyGenerator;
import org.apache.gobblin.broker.SharedResourcesBrokerFactory;
import org.apache.gobblin.broker.SimpleScopeType;
import org.apache.gobblin.broker.iface.NotConfiguredException;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;
import org.apache.gobblin.restli.SharedRestClientFactory;
import org.apache.gobblin.restli.SharedRestClientKey;
import org.apache.gobblin.util.ExecutorsUtils;
import org.apache.gobblin.util.limiter.Limiter;
import org.apache.gobblin.util.limiter.MultiLimiter;
import org.apache.gobblin.util.limiter.NoopLimiter;
import org.apache.gobblin.util.limiter.RateBasedLimiter;
import org.apache.gobblin.util.limiter.RestliLimiterFactory;
import org.apache.gobblin.util.limiter.broker.SharedLimiterKey;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;


/**
 * An MR job to test the performance of throttling.
 *
 * Each mapper runs a {@link Stressor}, which uses an {@link AtomicLong} to record its progress, and a {@link Limiter}
 * to throttle is progress. Different {@link Stressor}s might produce different usage patterns.
 *
 * The mappers emit a report every 15 seconds with the rate at which the {@link Stressor} is making progress (measured by
 * the rate at which the {@link AtomicLong} increases).
 *
 * The reducer computes the aggregate rate at which all {@link Stressor}s make progress.
 */
@Slf4j
public class MRStressTest {

  public static final String USE_THROTTLING_SERVER = "stressTest.useThrottlingServer";
  public static final String RESOURCE_ID = "stressTest.resourceLimited";
  public static final String LOCALLY_ENFORCED_QPS = "stressTest.localQps";

  public static final String NUM_MAPPERS = "stressTest.num.mappers";

  public static final String DEFAULT_MAPPERS = "10";

  public static final Option NUM_MAPPERS_OPT = new Option("mappers", true, "Num mappers");
  public static final Option THROTTLING_SERVER_URI = new Option("throttling", true, "Throttling server uri");
  public static final Option RESOURCE_ID_OPT = new Option("resource", true, "Resource id for throttling server");
  public static final Option LOCAL_QPS_OPT = new Option("localQps", true, "Locally enforced QPS");
  public static final Options OPTIONS = StressTestUtils.OPTIONS.addOption(NUM_MAPPERS_OPT).addOption(THROTTLING_SERVER_URI)
      .addOption(RESOURCE_ID_OPT).addOption(LOCAL_QPS_OPT);

  public static void main(String[] args) throws Exception {

    CommandLine cli = StressTestUtils.parseCommandLine(OPTIONS, args);

    Configuration configuration = new Configuration();
    if (cli.hasOption(THROTTLING_SERVER_URI.getOpt())) {
      configuration.setBoolean(USE_THROTTLING_SERVER, true);
      String resourceLimited = cli.getOptionValue(RESOURCE_ID_OPT.getOpt(), "MRStressTest");
      configuration.set(RESOURCE_ID, resourceLimited);
      configuration.set(
          BrokerConfigurationKeyGenerator.generateKey(new SharedRestClientFactory(),
              new SharedRestClientKey(RestliLimiterFactory.RESTLI_SERVICE_NAME),
              null, SharedRestClientFactory.SERVER_URI_KEY), cli.getOptionValue(THROTTLING_SERVER_URI.getOpt()));
    }

    if (cli.hasOption(LOCAL_QPS_OPT.getOpt())) {
      configuration .set(LOCALLY_ENFORCED_QPS, cli.getOptionValue(LOCAL_QPS_OPT.getOpt()));
    }

    Job job = Job.getInstance(configuration, "ThrottlingStressTest");
    job.getConfiguration().setBoolean("mapreduce.job.user.classpath.first", true);
    job.getConfiguration().setBoolean("mapreduce.map.speculative", false);

    job.getConfiguration().set(NUM_MAPPERS, cli.getOptionValue(NUM_MAPPERS_OPT.getOpt(), DEFAULT_MAPPERS));
    StressTestUtils.populateConfigFromCli(job.getConfiguration(), cli);

    job.setJarByClass(MRStressTest.class);
    job.setMapperClass(StresserMapper.class);
    job.setReducerClass(AggregatorReducer.class);
    job.setInputFormatClass(MyInputFormat.class);

    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(DoubleWritable.class);
    FileOutputFormat.setOutputPath(job, new Path("/tmp/MRStressTest" + System.currentTimeMillis()));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

  /**
   * Instantiates a {@link Stressor} and runs it until it exits. It also sets up a {@link Recorder} that computes and
   * records the rate at which the {@link AtomicLong} increases every 15 seconds.
   */
  public static class StresserMapper extends Mapper<Text, NullWritable, LongWritable, DoubleWritable> {
    private SharedResourcesBroker<SimpleScopeType> broker;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      Map<String, String> configMap = Maps.newHashMap();

      SharedResourcesBrokerFactory.addBrokerKeys(configMap, context.getConfiguration());
      this.broker = SharedResourcesBrokerFactory.createDefaultTopLevelBroker(ConfigFactory.parseMap(configMap),
          SimpleScopeType.GLOBAL.defaultScopeInstance());

      super.setup(context);
    }

    @Override
    protected void map(Text key, NullWritable value, Context context) throws IOException, InterruptedException {
      try {
        Configuration configuration = context.getConfiguration();

        Stressor stressor = context.getConfiguration().getClass(StressTestUtils.STRESSOR_CLASS,
            StressTestUtils.DEFAULT_STRESSOR_CLASS, Stressor.class).newInstance();
        stressor.configure(context.getConfiguration());

        RateComputingLimiterContainer limiterContainer = new RateComputingLimiterContainer();
        Limiter limiter = limiterContainer.decorateLimiter(createLimiter(configuration, this.broker));

        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        ScheduledFuture<?> future = executor.scheduleAtFixedRate(new Recorder(limiterContainer, context, true),
            0, 15, TimeUnit.SECONDS);

        limiter.start();
        stressor.run(limiter);
        limiter.stop();

        future.cancel(false);
        ExecutorsUtils.shutdownExecutorService(executor, Optional.<Logger>absent(), 10, TimeUnit.SECONDS);
      } catch (ReflectiveOperationException roe) {
        throw new IOException(roe);
      }
    }
  }

  /**
   * Simply adds up the rates for each key.
   */
  public static class AggregatorReducer extends Reducer<LongWritable, DoubleWritable, LongWritable, Text> {
    @Override
    protected void reduce(LongWritable key, Iterable<DoubleWritable> values, Context context)
        throws IOException, InterruptedException {
      double totalRate = 0;
      int activeMappers = 0;
      for (DoubleWritable value : values) {
        totalRate += value.get();
        activeMappers++;
      }
      context.write(key, new Text(String.format("%f\t%d", totalRate, activeMappers)));
    }
  }

  /**
   * Input format that just generates {@link #NUM_MAPPERS} dummy splits.
   */
  public static class MyInputFormat extends InputFormat<Text, NullWritable> {
    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
      int numMappers = context.getConfiguration().getInt(NUM_MAPPERS, 1);

      List<InputSplit> splits = Lists.newArrayList();
      for (int i = 0; i < numMappers; i++) {
        splits.add(new MySplit());
      }

      return splits;
    }

    @Override
    public RecordReader<Text, NullWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {
      return new MyRecordReader((MySplit) split);
    }
  }

  /**
   * A dummy {@link InputSplit}.
   */
  @Data
  public static class MySplit extends InputSplit implements Writable {

    @Override
    public long getLength() throws IOException, InterruptedException {
      return 1;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
      return new String[0];
    }

    @Override
    public void write(DataOutput out) throws IOException {
      Text.writeString(out, "split");
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      Text.readString(in);
    }
  }

  /**
   * A dummy {@link RecordReader} that emits a single key-value.
   */
  @RequiredArgsConstructor
  public static class MyRecordReader extends RecordReader<Text, NullWritable> {
    private final MySplit split;
    boolean keyValueAvailable = true;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      if (!this.keyValueAvailable) {
        return false;
      }
      this.keyValueAvailable = false;
      return true;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
      return new Text("split");
    }

    @Override
    public NullWritable getCurrentValue() throws IOException, InterruptedException {
      return NullWritable.get();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return 0;
    }

    @Override
    public void close() throws IOException {

    }
  }

  /**
   * A {@link Runnable} that computes the average rate at which the input {@link AtomicLong} increases and emits it to the
   * mapper collector.
   */
  @RequiredArgsConstructor
  private static class Recorder implements Runnable {
    private final RateComputingLimiterContainer limiter;
    private final MapContext<Text, NullWritable, LongWritable, DoubleWritable> context;
    private final boolean relativeKey;
    private int runs = -1;

    @Override
    public void run() {
      DescriptiveStatistics stats = this.limiter.getRateStatsSinceLastReport();
      long now = System.currentTimeMillis();
      this.runs++;

      if (stats != null) {
        long key;
        if (this.relativeKey) {
          key = 15 * this.runs;
        } else {
          DateTime nowTime = new DateTime(now).withMillisOfSecond(0);
          DateTime rounded = nowTime.withSecondOfMinute(15 * (nowTime.getSecondOfMinute() / 15));
          key = rounded.getMillis() / 1000;
        }


        try {
          this.context.write(new LongWritable(key), new DoubleWritable(stats.getSum()));
        } catch (IOException | InterruptedException ioe) {
          log.error("Error: ", ioe);
        }
      }

    }
  }

  static Limiter createLimiter(Configuration configuration, SharedResourcesBroker<SimpleScopeType> broker) {
    try {
      Limiter limiter = new NoopLimiter();

      long localQps = configuration.getLong(LOCALLY_ENFORCED_QPS, 0);
      if (localQps > 0) {
        log.info("Setting up local qps " + localQps);
        limiter = new MultiLimiter(limiter, new RateBasedLimiter(localQps));
      }

      if (configuration.getBoolean(USE_THROTTLING_SERVER, false)) {
        log.info("Setting up remote throttling.");
        String resourceId = configuration.get(RESOURCE_ID);
        Limiter globalLimiter =
            broker.getSharedResource(new RestliLimiterFactory<SimpleScopeType>(), new SharedLimiterKey(resourceId));
        limiter = new MultiLimiter(limiter, globalLimiter);
      }
      return limiter;
    } catch (NotConfiguredException nce) {
      throw new RuntimeException(nce);
    }
  }

}
