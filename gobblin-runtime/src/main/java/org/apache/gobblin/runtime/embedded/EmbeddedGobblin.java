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

package gobblin.runtime.embedded;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.avro.SchemaBuilder;
import org.apache.commons.lang3.ClassUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ClassUtil;
import org.joda.time.Period;
import org.joda.time.ReadableInstant;
import org.reflections.Reflections;
import org.slf4j.Logger;

import com.codahale.metrics.MetricFilter;
import com.github.rholder.retry.RetryListener;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.escape.Escaper;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.linkedin.data.template.DataTemplate;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.instrumented.extractor.InstrumentedExtractorBase;
import gobblin.metastore.FsStateStore;
import gobblin.metrics.GobblinMetrics;
import gobblin.metrics.MetricContext;
import gobblin.runtime.JobLauncherFactory;
import gobblin.runtime.Task;
import gobblin.runtime.api.Configurable;
import gobblin.runtime.api.GobblinInstanceDriver;
import gobblin.runtime.api.GobblinInstanceEnvironment;
import gobblin.runtime.api.GobblinInstancePluginFactory;
import gobblin.runtime.api.JobCatalog;
import gobblin.runtime.api.JobExecutionDriver;
import gobblin.runtime.api.JobExecutionResult;
import gobblin.runtime.api.JobSpec;
import gobblin.runtime.api.JobTemplate;
import gobblin.runtime.api.SpecNotFoundException;
import gobblin.runtime.cli.ConstructorAndPublicMethodsGobblinCliFactory;
import gobblin.runtime.cli.CliObjectOption;
import gobblin.runtime.cli.CliObjectSupport;
import gobblin.runtime.cli.NotOnCli;
import gobblin.runtime.instance.SimpleGobblinInstanceEnvironment;
import gobblin.runtime.instance.StandardGobblinInstanceDriver;
import gobblin.runtime.job_catalog.ImmutableFSJobCatalog;
import gobblin.runtime.job_catalog.PackagedTemplatesJobCatalogDecorator;
import gobblin.runtime.job_catalog.StaticJobCatalog;
import gobblin.runtime.job_spec.ResolvedJobSpec;
import gobblin.runtime.plugins.GobblinInstancePluginUtils;
import gobblin.runtime.plugins.PluginStaticKeys;
import gobblin.runtime.plugins.metrics.GobblinMetricsPlugin;
import gobblin.runtime.std.DefaultConfigurableImpl;
import gobblin.runtime.std.DefaultJobLifecycleListenerImpl;
import gobblin.state.ConstructState;
import gobblin.util.HadoopUtils;
import gobblin.util.PathUtils;
import gobblin.util.PullFileLoader;

import javassist.bytecode.ClassFile;
import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


/**
 * A class used to run an embedded version of Gobblin. This class is only intended for running a single Gobblin job.
 * If a large number of Gobblin jobs will be launched, use a {@link GobblinInstanceDriver} instead.
 *
 * Usage:
 * new EmbeddedGobblin("jobName").setTemplate(myTemplate).setConfiguration("key","value").run();
 */
@Slf4j
public class EmbeddedGobblin {

  public static class CliFactory extends ConstructorAndPublicMethodsGobblinCliFactory {
    public CliFactory() {
      super(EmbeddedGobblin.class);
    }

    @Override
    public String getUsageString() {
      return "-jobName <jobName> [OPTIONS]";
    }
  }

  private static final Splitter KEY_VALUE_SPLITTER = Splitter.on(":").limit(2);

  private final JobSpec.Builder specBuilder;
  private final Map<String, String> userConfigMap;
  private final Map<String, String> builtConfigMap;
  private final Config defaultSysConfig;
  private final Map<String, String> sysConfigOverrides;
  private final Map<String, Integer> distributedJars;
  private Runnable distributeJarsFunction;
  private JobTemplate template;
  private Logger useLog = log;
  private FullTimeout launchTimeout = new FullTimeout(10, TimeUnit.SECONDS);
  private FullTimeout jobTimeout = new FullTimeout(10, TimeUnit.DAYS);
  private FullTimeout shutdownTimeout = new FullTimeout(10, TimeUnit.SECONDS);
  private List<GobblinInstancePluginFactory> plugins = Lists.newArrayList();
  private Optional<Path> jobFile = Optional.absent();

  public EmbeddedGobblin() {
    this("EmbeddedGobblin");
  }

  @CliObjectSupport(argumentNames = {"jobName"})
  public EmbeddedGobblin(String name) {
    HadoopUtils.addGobblinSite();
    this.specBuilder = new JobSpec.Builder(name);
    this.userConfigMap = Maps.newHashMap();
    this.builtConfigMap = Maps.newHashMap();
    this.sysConfigOverrides = Maps.newHashMap();
    this.defaultSysConfig = getDefaultSysConfig();
    this.distributedJars = Maps.newHashMap();
    loadCoreGobblinJarsToDistributedJars();
    this.distributeJarsFunction = new Runnable() {
      @Override
      public void run() {
        // NOOP
      }
    };
  }

  /**
   * Specify job should run in MR mode.
   */
  public EmbeddedGobblin mrMode() throws IOException {
    this.sysConfigOverrides.put(ConfigurationKeys.JOB_LAUNCHER_TYPE_KEY, JobLauncherFactory.JobLauncherType.MAPREDUCE.name());
    this.builtConfigMap.put(ConfigurationKeys.FS_URI_KEY, FileSystem.get(new Configuration()).getUri().toString());
    this.builtConfigMap.put(ConfigurationKeys.MR_JOB_ROOT_DIR_KEY, "/tmp/EmbeddedGobblin_" + System.currentTimeMillis());
    this.distributeJarsFunction = new Runnable() {
      @Override
      public void run() {
        // Add jars needed at runtime to the sys config so MR job launcher will add them to distributed cache.
        EmbeddedGobblin.this.sysConfigOverrides.put(ConfigurationKeys.JOB_JAR_FILES_KEY,
            Joiner.on(",").join(getPrioritizedDistributedJars()));
      }
    };
    return this;
  }

  /**
   * Specify that the input jar should be added to workers' classpath on distributed mode.
   */
  public EmbeddedGobblin distributeJar(String jarPath) {
    return distributeJarWithPriority(jarPath, 0);
  }

  /**
   * Specify that the input jar should be added to workers' classpath on distributed mode. Jars with lower priority value
   * will appear first in the classpath. Default priority is 0.
   */
  public EmbeddedGobblin distributeJarByClassWithPriority(Class<?> klazz, int priority) {
    return distributeJarWithPriority(ClassUtil.findContainingJar(klazz), priority);
  }

  /**
   * Specify that the input jar should be added to workers' classpath on distributed mode. Jars with lower priority value
   * will appear first in the classpath. Default priority is 0.
   */
  public synchronized EmbeddedGobblin distributeJarWithPriority(String jarPath, int priority) {
    if (this.distributedJars.containsKey(jarPath)) {
      this.distributedJars.put(jarPath, Math.min(priority, this.distributedJars.get(jarPath)));
    } else {
      this.distributedJars.put(jarPath, priority);
    }
    return this;
  }

  /**
   * Set a {@link JobTemplate} to use.
   */
  public EmbeddedGobblin setTemplate(JobTemplate template) {
    this.template = template;
    return this;
  }

  /**
   * Set a {@link JobTemplate} to use.
   */
  public EmbeddedGobblin setTemplate(String templateURI) throws URISyntaxException, SpecNotFoundException,
                                                        JobTemplate.TemplateException {
    return setTemplate(new PackagedTemplatesJobCatalogDecorator().getTemplate(new URI(templateURI)));
  }

  /**
   * Use a {@link gobblin.runtime.api.GobblinInstancePlugin}.
   */
  public EmbeddedGobblin usePlugin(GobblinInstancePluginFactory pluginFactory) {
    this.plugins.add(pluginFactory);
    return this;
  }

  /**
   * Use a {@link gobblin.runtime.api.GobblinInstancePlugin} identified by name.
   */
  public EmbeddedGobblin usePlugin(String pluginAlias) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
    return usePlugin(GobblinInstancePluginUtils.instantiatePluginByAlias(pluginAlias));
  }

  /**
   * Override a Gobblin system configuration.
   */
  public EmbeddedGobblin sysConfig(String key, String value) {
    this.sysConfigOverrides.put(key, value);
    return this;
  }

  /**
   * Override a Gobblin system configuration. Format "<key>:<value>"
   */
  public EmbeddedGobblin sysConfig(String keyValue) {
    List<String> split = KEY_VALUE_SPLITTER.splitToList(keyValue);
    if (split.size() != 2) {
      throw new RuntimeException("Cannot parse " + keyValue + ". Expected <key>:<value>.");
    }
    return sysConfig(split.get(0), split.get(1));
  }

  /**
   * Provide a job file.
   */
  public EmbeddedGobblin jobFile(String pathStr) {
    this.jobFile = Optional.of(new Path(pathStr));
    return this;
  }

  /**
   * Load Kerberos keytab for authentication. Crendetials format "<login-user>:<keytab-file>".
   */
  @CliObjectOption(description = "Authenticate using kerberos. Format: \"<login-user>:<keytab-file>\".")
  public EmbeddedGobblin kerberosAuthentication(String credentials) {
    List<String> split = Splitter.on(":").splitToList(credentials);
    if (split.size() != 2) {
      throw new RuntimeException("Cannot parse " + credentials + ". Expected <login-user>:<keytab-file>");
    }
    try {
      usePlugin(PluginStaticKeys.HADOOP_LOGIN_FROM_KEYTAB_ALIAS);
    } catch (ReflectiveOperationException roe) {
      throw new RuntimeException(String.format("Could not instantiate %s. Make sure gobblin-runtime-hadoop is in your classpath.",
          PluginStaticKeys.HADOOP_LOGIN_FROM_KEYTAB_ALIAS), roe);
    }
    sysConfig(PluginStaticKeys.LOGIN_USER_FULL_KEY, split.get(0));
    sysConfig(PluginStaticKeys.LOGIN_USER_KEYTAB_FILE_FULL_KEY, split.get(1));
    return this;
  }

  /**
   * Manually set a key-value pair in the job configuration.
   */
  public EmbeddedGobblin setConfiguration(String key, String value) {
    this.userConfigMap.put(key, value);
    return this;
  }

  /**
   * Manually set a key-value pair in the job configuration. Input is of the form <key>:<value>
   */
  public EmbeddedGobblin setConfiguration(String keyValue) {
    List<String> split = KEY_VALUE_SPLITTER.splitToList(keyValue);
    if (split.size() != 2) {
      throw new RuntimeException("Cannot parse " + keyValue + ". Expected <key>:<value>.");
    }
    return setConfiguration(split.get(0), split.get(1));
  }

  /**
   * Set the timeout for the Gobblin job execution.
   */
  public EmbeddedGobblin setJobTimeout(long timeout, TimeUnit timeUnit) {
    this.jobTimeout = new FullTimeout(timeout, timeUnit);
    return this;
  }

  /**
   * Set the timeout for the Gobblin job execution from ISO-style period.
   */
  public EmbeddedGobblin setJobTimeout(String timeout) {
    return setJobTimeout(Period.parse(timeout).getSeconds(), TimeUnit.SECONDS);
  }

  /**
   * Set the timeout for launching the Gobblin job.
   */
  public EmbeddedGobblin setLaunchTimeout(long timeout, TimeUnit timeUnit) {
    this.launchTimeout = new FullTimeout(timeout, timeUnit);
    return this;
  }

  /**
   * Set the timeout for launching the Gobblin job from ISO-style period.
   */
  public EmbeddedGobblin setLaunchTimeout(String timeout) {
    return setLaunchTimeout(Period.parse(timeout).getSeconds(), TimeUnit.SECONDS);
  }

  /**
   * Set the timeout for shutting down the Gobblin instance driver after the job is done.
   */
  public EmbeddedGobblin setShutdownTimeout(long timeout, TimeUnit timeUnit) {
    this.shutdownTimeout = new FullTimeout(timeout, timeUnit);
    return this;
  }

  /**
   * Set the timeout for shutting down the Gobblin instance driver after the job is done from ISO-style period.
   */
  public EmbeddedGobblin setShutdownTimeout(String timeout) {
    return setShutdownTimeout(Period.parse(timeout).getSeconds(), TimeUnit.SECONDS);
  }

  /**
   * Enable state store.
   */
  public EmbeddedGobblin useStateStore(String rootDir) {
    this.setConfiguration(ConfigurationKeys.STATE_STORE_ENABLED, "true");
    this.setConfiguration(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY, rootDir);
    return this;
  }

  /**
   * Enable metrics. Does not start any reporters.
   */
  public EmbeddedGobblin enableMetrics() {
    this.usePlugin(new GobblinMetricsPlugin.Factory());
    this.sysConfig(ConfigurationKeys.METRICS_ENABLED_KEY, Boolean.toString(true));
    return this;
  }

  /**
   * This is the base {@link Config} used for the job, containing all default configurations. Subclasses can override
   * default configurations (for example setting a particular {@link gobblin.runtime.JobLauncherFactory.JobLauncherType}.
   */
  protected Config getDefaultSysConfig() {
    return ConfigFactory.parseResources("embedded/embedded.conf");
  }

  /**
   * Run the Gobblin job. This call will block until the job is done.
   * @return a {@link JobExecutionResult} containing the result of the execution.
   */
  @NotOnCli
  public JobExecutionResult run() throws InterruptedException, TimeoutException, ExecutionException {
    JobExecutionDriver jobDriver = runAsync();
    return jobDriver.get(this.jobTimeout.getTimeout(), this.jobTimeout.getTimeUnit());
  }

  /**
   * Launch the Gobblin job asynchronously. This method will return when the Gobblin job has started.
   * @return a {@link JobExecutionDriver}. This object is a future that will resolve when the Gobblin job finishes.
   * @throws TimeoutException if the Gobblin job does not start within the launch timeout.
   */
  @NotOnCli
  public JobExecutionDriver runAsync() throws TimeoutException, InterruptedException {
    // Run function to distribute jars to workers in distributed mode
    this.distributeJarsFunction.run();

    Config sysProps = ConfigFactory.parseMap(this.builtConfigMap)
        .withFallback(this.defaultSysConfig);
    Config userConfig = ConfigFactory.parseMap(this.userConfigMap);

    JobSpec jobSpec;
    if (this.jobFile.isPresent()) {
      try {
        Path jobFilePath = this.jobFile.get();
        PullFileLoader loader =
            new PullFileLoader(jobFilePath.getParent(), jobFilePath.getFileSystem(new Configuration()),
                PullFileLoader.DEFAULT_JAVA_PROPS_PULL_FILE_EXTENSIONS,
                PullFileLoader.DEFAULT_HOCON_PULL_FILE_EXTENSIONS);
        Config jobConfig = userConfig.withFallback(loader.loadPullFile(jobFilePath, sysProps, false));
        ImmutableFSJobCatalog.JobSpecConverter converter =
            new ImmutableFSJobCatalog.JobSpecConverter(jobFilePath.getParent(), Optional.<String>absent());
        jobSpec = converter.apply(jobConfig);
      } catch (IOException ioe) {
        throw new RuntimeException("Failed to run embedded Gobblin.", ioe);
      }
    } else {
      Config finalConfig = userConfig.withFallback(sysProps);
      if (this.template != null) {
        try {
          finalConfig = this.template.getResolvedConfig(finalConfig);
        } catch (SpecNotFoundException | JobTemplate.TemplateException exc) {
          throw new RuntimeException(exc);
        }
      }
      jobSpec = this.specBuilder.withConfig(finalConfig).build();
    }

    ResolvedJobSpec resolvedJobSpec;
    try {
      resolvedJobSpec = new ResolvedJobSpec(jobSpec);
    } catch (SpecNotFoundException | JobTemplate.TemplateException exc) {
      throw new RuntimeException("Failed to resolved template.", exc);
    }
    final JobCatalog jobCatalog = new StaticJobCatalog(Optional.of(this.useLog), Lists.<JobSpec>newArrayList(resolvedJobSpec));

    SimpleGobblinInstanceEnvironment instanceEnvironment =
        new SimpleGobblinInstanceEnvironment("EmbeddedGobblinInstance", this.useLog, getSysConfig());

    StandardGobblinInstanceDriver.Builder builder =
        new StandardGobblinInstanceDriver.Builder(Optional.<GobblinInstanceEnvironment>of(instanceEnvironment)).withLog(this.useLog)
        .withJobCatalog(jobCatalog)
        .withImmediateJobScheduler();

    for (GobblinInstancePluginFactory plugin : this.plugins) {
      builder.addPlugin(plugin);
    }

    final GobblinInstanceDriver driver = builder.build();

    EmbeddedJobLifecycleListener listener = new EmbeddedJobLifecycleListener(this.useLog);
    driver.registerJobLifecycleListener(listener);

    driver.startAsync();

    boolean started = listener.awaitStarted(this.launchTimeout.getTimeout(), this.launchTimeout.getTimeUnit());
    if (!started) {
      log.warn("Timeout waiting for job to start. Aborting.");
      driver.stopAsync();
      driver.awaitTerminated(this.shutdownTimeout.getTimeout(), this.shutdownTimeout.getTimeUnit());
      throw new TimeoutException("Timeout waiting for job to start.");
    }

    final JobExecutionDriver jobDriver = listener.getJobDriver();
    // Stop the Gobblin instance driver when the job finishes.
    Futures.addCallback(jobDriver, new FutureCallback<JobExecutionResult>() {
      @Override
      public void onSuccess(@Nullable JobExecutionResult result) {

        stopGobblinInstanceDriver();
      }

      @Override
      public void onFailure(Throwable t) {
        stopGobblinInstanceDriver();
      }

      private void stopGobblinInstanceDriver() {
        try {
          driver.stopAsync();
          driver.awaitTerminated(EmbeddedGobblin.this.shutdownTimeout.getTimeout(), EmbeddedGobblin.this.shutdownTimeout
              .getTimeUnit());
        } catch (TimeoutException te) {
          log.error("Failed to shutdown Gobblin instance driver.");
        }
      }
    });

    return listener.getJobDriver();
  }

  private Configurable getSysConfig() {
    return DefaultConfigurableImpl.createFromConfig(ConfigFactory.parseMap(this.sysConfigOverrides).withFallback(this.defaultSysConfig));
  }

  /**
   * This returns the set of jars required by a basic Gobblin ingestion job. In general, these need to be distributed
   * to workers in a distributed environment.
   */
  private void loadCoreGobblinJarsToDistributedJars() {
    // Gobblin-api
    distributeJarByClassWithPriority(State.class, 0);
    // Gobblin-core
    distributeJarByClassWithPriority(ConstructState.class, 0);
    // Gobblin-core-base
    distributeJarByClassWithPriority(InstrumentedExtractorBase.class, 0);
    // Gobblin-metrics-base
    distributeJarByClassWithPriority(MetricContext.class, 0);
    // Gobblin-metrics
    distributeJarByClassWithPriority(GobblinMetrics.class, 0);
    // Gobblin-metastore
    distributeJarByClassWithPriority(FsStateStore.class, 0);
    // Gobblin-runtime
    distributeJarByClassWithPriority(Task.class, 0);
    // Gobblin-utility
    distributeJarByClassWithPriority(PathUtils.class, 0);
    // joda-time
    distributeJarByClassWithPriority(ReadableInstant.class, 0);
    // guava
    distributeJarByClassWithPriority(Escaper.class, -10); // Escaper was added in guava 15, so we use it to identify correct jar
    // dropwizard.metrics-core
    distributeJarByClassWithPriority(MetricFilter.class, 0);
    // pegasus
    distributeJarByClassWithPriority(DataTemplate.class, 0);
    // commons-lang3
    distributeJarByClassWithPriority(ClassUtils.class, 0);
    // avro
    distributeJarByClassWithPriority(SchemaBuilder.class, 0);
    // guava-retry
    distributeJarByClassWithPriority(RetryListener.class, 0);
    // config
    distributeJarByClassWithPriority(ConfigFactory.class, 0);
    // reflections
    distributeJarByClassWithPriority(Reflections.class, 0);
    // javassist
    distributeJarByClassWithPriority(ClassFile.class, 0);
  }

  /**
   * Encapsulates a timeout with corresponding {@link TimeUnit}.
   */
  @Data
  private static class FullTimeout {
    private final long timeout;
    private final TimeUnit timeUnit;
  }

  @VisibleForTesting
  protected List<String> getPrioritizedDistributedJars() {
    List<Map.Entry<String, Integer>> jarsWithPriority = Lists.newArrayList(this.distributedJars.entrySet());
    Collections.sort(jarsWithPriority, new Comparator<Map.Entry<String, Integer>>() {
      @Override
      public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
        return Integer.compare(o1.getValue(), o2.getValue());
      }
    });
    return Lists.transform(jarsWithPriority, new Function<Map.Entry<String, Integer>, String>() {
      @Override
      public String apply(Map.Entry<String, Integer> input) {
        return input.getKey();
      }
    });
  }

  /**
   * A {@link gobblin.runtime.api.JobLifecycleListener} that listens for a particular job and detects the start of the job.
   */
  private static class EmbeddedJobLifecycleListener extends DefaultJobLifecycleListenerImpl {

    private final Lock lock = new ReentrantLock();
    private final Condition runningStateCondition = this.lock.newCondition();
    private volatile boolean running = false;
    @Getter(value = AccessLevel.PRIVATE)
    private JobExecutionDriver jobDriver;

    public EmbeddedJobLifecycleListener(Logger log) {
      super(log);
    }

    /**
     * Block until the job has started.
     * @return true if the job started, false on timeout.
     */
    public boolean awaitStarted(long timeout, TimeUnit timeUnit) throws InterruptedException {
      this.lock.lock();
      try {

        long startTime = System.currentTimeMillis();
        long totalTimeMillis = timeUnit.toMillis(timeout);

        while (!running) {
          long millisLeft = totalTimeMillis - (System.currentTimeMillis() - startTime);
          if (millisLeft < 0) {
            return false;
          }
          boolean outoftime = this.runningStateCondition.await(millisLeft, TimeUnit.MILLISECONDS);
        }
      } finally {
        this.lock.unlock();
      }
      return true;
    }

    @Override
    public void onJobLaunch(JobExecutionDriver jobDriver) {
      if (this.jobDriver != null) {
        throw new IllegalStateException("OnJobLaunch called when a job was already running.");
      }
      super.onJobLaunch(jobDriver);
      this.lock.lock();
      try {
        this.running = true;
        this.jobDriver = jobDriver;
        this.runningStateCondition.signal();
      } finally {
        this.lock.unlock();
      }
    }
  }

}
