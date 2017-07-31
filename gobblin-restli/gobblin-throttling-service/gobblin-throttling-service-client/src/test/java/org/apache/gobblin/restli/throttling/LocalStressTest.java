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

package org.apache.gobblin.restli.throttling;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Maps;
import com.google.inject.Key;
import com.google.inject.name.Names;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.broker.BrokerConfigurationKeyGenerator;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;
import org.apache.gobblin.util.limiter.Limiter;
import org.apache.gobblin.util.limiter.MockRequester;
import org.apache.gobblin.util.limiter.RestliServiceBasedLimiter;
import org.apache.gobblin.util.limiter.broker.SharedLimiterKey;
import org.apache.gobblin.util.limiter.stressTest.RateComputingLimiterContainer;
import org.apache.gobblin.util.limiter.stressTest.StressTestUtils;
import org.apache.gobblin.util.limiter.stressTest.Stressor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;


/**
 * A stress test for throttling service. It creates a number of threads, each one running a stressor using a mock
 * {@link RestliServiceBasedLimiter}.
 *
 * The mock {@link RestliServiceBasedLimiter} sends requests to an embedded {@link LimiterServerResource}, adding an
 * artificial latency to the requests representing the network latency.
 *
 * The stress test prints permit granting statistics every 15 seconds.
 */
@Slf4j
public class LocalStressTest {

  public static final Option STRESSOR_THREADS =
      new Option("stressorThreads", true, "Number of stressor threads");
  public static final Option PROCESSOR_THREADS =
      new Option("processorThreads", true, "Number of request processor threads.");
  public static final Option ARTIFICIAL_LATENCY =
      new Option("latency", true, "Artificial request latency in millis.");
  public static final Option QPS =
      new Option("qps", true, "Target qps.");

  public static final Options OPTIONS = StressTestUtils.OPTIONS.addOption(STRESSOR_THREADS).addOption(PROCESSOR_THREADS);

  public static final int DEFAULT_STRESSOR_THREADS = 10;
  public static final int DEFAULT_PROCESSOR_THREADS = 10;
  public static final int DEFAULT_ARTIFICIAL_LATENCY = 100;
  public static final int DEFAULT_TARGET_QPS = 100;

  public static void main(String[] args) throws Exception {

    CommandLine cli = StressTestUtils.parseCommandLine(OPTIONS, args);

    int stressorThreads = Integer.parseInt(cli.getOptionValue(STRESSOR_THREADS.getOpt(), Integer.toString(
        DEFAULT_STRESSOR_THREADS)));
    int processorThreads = Integer.parseInt(cli.getOptionValue(PROCESSOR_THREADS.getOpt(), Integer.toString(
        DEFAULT_PROCESSOR_THREADS)));
    int artificialLatency = Integer.parseInt(cli.getOptionValue(ARTIFICIAL_LATENCY.getOpt(), Integer.toString(
        DEFAULT_ARTIFICIAL_LATENCY)));
    long targetQps = Integer.parseInt(cli.getOptionValue(QPS.getOpt(), Integer.toString(
        DEFAULT_TARGET_QPS)));

    Configuration configuration = new Configuration();
    StressTestUtils.populateConfigFromCli(configuration, cli);

    String resourceLimited = LocalStressTest.class.getSimpleName();

    Map<String, String> configMap = Maps.newHashMap();

    ThrottlingPolicyFactory factory = new ThrottlingPolicyFactory();
    SharedLimiterKey res1key = new SharedLimiterKey(resourceLimited);
    configMap.put(BrokerConfigurationKeyGenerator.generateKey(factory, res1key, null, ThrottlingPolicyFactory.POLICY_KEY),
            QPSPolicy.FACTORY_ALIAS);
    configMap.put(BrokerConfigurationKeyGenerator.generateKey(factory, res1key, null, QPSPolicy.QPS),
        Long.toString(targetQps));

    ThrottlingGuiceServletConfig guiceServletConfig = new ThrottlingGuiceServletConfig();
    guiceServletConfig.initialize(ConfigFactory.parseMap(configMap));
    LimiterServerResource limiterServer = guiceServletConfig.getInjector().getInstance(LimiterServerResource.class);

    RateComputingLimiterContainer limiterContainer = new RateComputingLimiterContainer();

    Class<? extends Stressor> stressorClass =
        configuration.getClass(StressTestUtils.STRESSOR_CLASS, StressTestUtils.DEFAULT_STRESSOR_CLASS, Stressor.class);

    ExecutorService executorService = Executors.newFixedThreadPool(stressorThreads);

    SharedResourcesBroker broker =
        guiceServletConfig.getInjector().getInstance(Key.get(SharedResourcesBroker.class, Names.named(LimiterServerResource.BROKER_INJECT_NAME)));
    ThrottlingPolicy policy = (ThrottlingPolicy) broker.getSharedResource(new ThrottlingPolicyFactory(),
        new SharedLimiterKey(resourceLimited));
    ScheduledExecutorService reportingThread = Executors.newSingleThreadScheduledExecutor();
    reportingThread.scheduleAtFixedRate(new Reporter(limiterContainer, policy), 0, 15, TimeUnit.SECONDS);

    Queue<Future<?>> futures = new LinkedList<>();
    MockRequester requester = new MockRequester(limiterServer, artificialLatency, processorThreads);

    requester.start();
    for (int i = 0; i < stressorThreads; i++) {
      RestliServiceBasedLimiter restliLimiter = RestliServiceBasedLimiter.builder().resourceLimited(resourceLimited)
          .requestSender(requester)
          .serviceIdentifier("stressor" + i).build();

      Stressor stressor = stressorClass.newInstance();
      stressor.configure(configuration);
      futures.add(executorService.submit(new StressorRunner(limiterContainer.decorateLimiter(restliLimiter),
          stressor)));
    }
    int stressorFailures = 0;
    for (Future<?> future : futures) {
      try {
        future.get();
      } catch (ExecutionException ee) {
        stressorFailures++;
      }
    }
    requester.stop();

    executorService.shutdownNow();

    if (stressorFailures > 0) {
      log.error("There were " + stressorFailures + " failed stressor threads.");
    }
    System.exit(stressorFailures);
  }

  @RequiredArgsConstructor
  private static class StressorRunner implements Runnable {
    private final Limiter limiter;
    private final Stressor stressor;

    @Override
    public void run() {
      try {
        this.limiter.start();
        this.stressor.run(this.limiter);
        this.limiter.stop();
      } catch (InterruptedException ie) {
        log.error("Error: ", ie);
      }
    }
  }

  @RequiredArgsConstructor
  private static class Reporter implements Runnable {
    private final RateComputingLimiterContainer limiter;
    private final ThrottlingPolicy policy;

    @Override
    public void run() {
      DescriptiveStatistics stats = limiter.getRateStatsSinceLastReport();
      if (stats != null) {
        log.info(String.format("Requests rate stats: count: %d, min: %f, max: %f, mean: %f, std: %f, sum: %f", stats.getN(),
            stats.getMin(), stats.getMax(), stats.getMean(), stats.getStandardDeviation(), stats.getSum()));
      }

      stats = limiter.getUnusedPermitsSinceLastReport();
      if (stats != null) {
        log.info(String.format("Unused permits rate stats: count: %d, min: %f, max: %f, mean: %f, std: %f, sum: %f", stats.getN(),
            stats.getMin(), stats.getMax(), stats.getMean(), stats.getStandardDeviation(), stats.getSum()));
      }

      if (this.policy instanceof QPSPolicy) {
        QPSPolicy qpsPolicy = (QPSPolicy) this.policy;
        DynamicTokenBucket dynamicTokenBucket = qpsPolicy.getTokenBucket();
        TokenBucket tokenBucket = dynamicTokenBucket.getTokenBucket();
        log.info("Stored tokens: " + tokenBucket.getStoredTokens());
      }
    }
  }
}
