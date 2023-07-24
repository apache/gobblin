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

package org.apache.gobblin.cluster;

import java.io.File;
import java.io.IOException;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.Service;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowClientOptions;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.cluster.event.ClusterManagerShutdownRequest;
import org.apache.gobblin.cluster.temporal.GobblinTemporalWorkflow;
import org.apache.gobblin.cluster.temporal.Shared;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.instrumented.StandardMetricsBridge;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.runtime.api.MutableJobCatalog;
import org.apache.gobblin.runtime.app.ApplicationException;
import org.apache.gobblin.runtime.app.ApplicationLauncher;
import org.apache.gobblin.runtime.app.ServiceBasedAppLauncher;
import org.apache.gobblin.scheduler.SchedulerService;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.JvmUtils;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;

import static org.apache.gobblin.security.ssl.SSLContextFactory.toInputStream;


/**
 * The central cluster manager for Gobblin Clusters.
 *
 *
 * <p>
 *   This class will initiates a graceful shutdown of the cluster in the following conditions:
 *
 *   <ul>
 *     <li>A shutdown request is received via a Helix message of subtype
 *     {@link HelixMessageSubTypes#APPLICATION_MASTER_SHUTDOWN}. Upon receiving such a message,
 *     it will call {@link #stop()} to initiate a graceful shutdown of the cluster</li>
 *     <li>The shutdown hook gets called. The shutdown hook will call {@link #stop()}, which will
 *     start a graceful shutdown of the cluster.</li>
 *   </ul>
 * </p>
 *
 * @author Yinan Li
 */
@Alpha
@Slf4j
public class GobblinTemporalClusterManager implements ApplicationLauncher, StandardMetricsBridge, LeadershipChangeAwareComponent {

  private static final Logger LOGGER = LoggerFactory.getLogger(GobblinTemporalClusterManager.class);

  private StopStatus stopStatus = new StopStatus(false);

  protected ServiceBasedAppLauncher applicationLauncher;

  // An EventBus used for communications between services running in the ApplicationMaster
  @Getter(AccessLevel.PUBLIC)
  protected final EventBus eventBus = new EventBus(GobblinTemporalClusterManager.class.getSimpleName());

  protected final Path appWorkDir;

  @Getter
  protected final FileSystem fs;

  protected final String applicationId;

  // thread used to keep process up for an idle controller
  private Thread idleProcessThread;

  // set to true to stop the idle process thread
  private volatile boolean stopIdleProcessThread = false;

  private final boolean isStandaloneMode;

  @Getter
  private MutableJobCatalog jobCatalog;
  @Getter
  private JobConfigurationManager jobConfigurationManager;
  @Getter
  private volatile boolean started = false;

  protected final String clusterName;
  @Getter
  protected final Config config;

  public GobblinTemporalClusterManager(String clusterName, String applicationId, Config sysConfig,
      Optional<Path> appWorkDirOptional) throws Exception {
    // Set system properties passed in via application config. As an example, Helix uses System#getProperty() for ZK configuration
    // overrides such as sessionTimeout. In this case, the overrides specified
    // in the application configuration have to be extracted and set before initializing HelixManager.
    GobblinClusterUtils.setSystemProperties(sysConfig);

    //Add dynamic config
    this.config = GobblinClusterUtils.addDynamicConfig(sysConfig);

    this.clusterName = clusterName;
    this.isStandaloneMode = ConfigUtils.getBoolean(this.config, GobblinClusterConfigurationKeys.STANDALONE_CLUSTER_MODE_KEY,
        GobblinClusterConfigurationKeys.DEFAULT_STANDALONE_CLUSTER_MODE);

    this.applicationId = applicationId;

    this.fs = GobblinClusterUtils.buildFileSystem(this.config, new Configuration());
    this.appWorkDir = appWorkDirOptional.isPresent() ? appWorkDirOptional.get()
        : GobblinClusterUtils.getAppWorkDirPathFromConfig(this.config, this.fs, clusterName, applicationId);
    LOGGER.info("Configured GobblinClusterManager work dir to: {}", this.appWorkDir);

    initializeAppLauncherAndServices();
  }

  /**
   * Create the service based application launcher and other associated services
   * @throws Exception
   */
  private void initializeAppLauncherAndServices() throws Exception {
    // Done to preserve backwards compatibility with the previously hard-coded timeout of 5 minutes
    Properties properties = ConfigUtils.configToProperties(this.config);
    if (!properties.contains(ServiceBasedAppLauncher.APP_STOP_TIME_SECONDS)) {
      properties.setProperty(ServiceBasedAppLauncher.APP_STOP_TIME_SECONDS, Long.toString(300));
    }
    this.applicationLauncher = new ServiceBasedAppLauncher(properties, this.clusterName);

    // create a job catalog for keeping track of received jobs if a job config path is specified
    if (this.config.hasPath(GobblinClusterConfigurationKeys.GOBBLIN_CLUSTER_PREFIX
        + ConfigurationKeys.JOB_CONFIG_FILE_GENERAL_PATH_KEY)) {
      String jobCatalogClassName = ConfigUtils.getString(config, GobblinClusterConfigurationKeys.JOB_CATALOG_KEY,
          GobblinClusterConfigurationKeys.DEFAULT_JOB_CATALOG);

      this.jobCatalog =
          (MutableJobCatalog) GobblinConstructorUtils.invokeFirstConstructor(Class.forName(jobCatalogClassName),
          ImmutableList.of(config
              .getConfig(StringUtils.removeEnd(GobblinClusterConfigurationKeys.GOBBLIN_CLUSTER_PREFIX, "."))
              .withFallback(this.config)));
    } else {
      this.jobCatalog = null;
    }

    SchedulerService schedulerService = new SchedulerService(properties);
    this.applicationLauncher.addService(schedulerService);
    this.jobConfigurationManager = buildJobConfigurationManager(config);
    this.applicationLauncher.addService(this.jobConfigurationManager);

    if (ConfigUtils.getBoolean(this.config, GobblinClusterConfigurationKeys.CONTAINER_HEALTH_METRICS_SERVICE_ENABLED,
        GobblinClusterConfigurationKeys.DEFAULT_CONTAINER_HEALTH_METRICS_SERVICE_ENABLED)) {
      this.applicationLauncher.addService(new ContainerHealthMetricsService(config));
    }
  }

  /**
   * Start any services required by the application launcher then start the application launcher
   */
  private void startAppLauncherAndServices() {
    // other services such as the job configuration manager have a dependency on the job catalog, so it has be be
    // started first
    if (this.jobCatalog instanceof Service) {
      ((Service) this.jobCatalog).startAsync().awaitRunning();
    }

    this.applicationLauncher.start();
  }

  /**
   * Stop the application launcher then any services that were started outside of the application launcher
   */
  private void stopAppLauncherAndServices() {
    try {
      this.applicationLauncher.stop();
    } catch (ApplicationException ae) {
      LOGGER.error("Error while stopping Gobblin Cluster application launcher", ae);
    }

    if (this.jobCatalog instanceof Service) {
      ((Service) this.jobCatalog).stopAsync().awaitTerminated();
    }
  }


  /**
   * Start the Gobblin Cluster Manager.
   */
  // @Import(clazz = ClientSslContextFactory.class, prefix = ClientSslContextFactory.SCOPE_PREFIX)
  @Override
  public void start() {
    // temporal workflow
    LOGGER.info("Starting the Gobblin Temporal Cluster Manager");

    this.eventBus.register(this);

    if (this.isStandaloneMode) {
      // standalone mode starts non-daemon threads later, so need to have this thread to keep process up
      this.idleProcessThread = new Thread(new Runnable() {
        @Override
        public void run() {
          while (!GobblinTemporalClusterManager.this.stopStatus.isStopInProgress() && !GobblinTemporalClusterManager.this.stopIdleProcessThread) {
            try {
              Thread.sleep(300);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              break;
            }
          }
        }
      });

      this.idleProcessThread.start();

      // Need this in case a kill is issued to the process so that the idle thread does not keep the process up
      // since GobblinClusterManager.stop() is not called this case.
      Runtime.getRuntime().addShutdownHook(new Thread(() -> GobblinTemporalClusterManager.this.stopIdleProcessThread = true));
    } else {
      startAppLauncherAndServices();
    }
    this.started = true;

    try {
      initiateWorkflow();
    }catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void initiateWorkflow()
      throws Exception {
    LOGGER.info("Initiating Temporal Workflow");
    WorkflowServiceStubs workflowServiceStubs = createServiceStubs();
    WorkflowClient client =
        WorkflowClient.newInstance(
            workflowServiceStubs, WorkflowClientOptions.newBuilder().setNamespace("gobblin-fastingest-internpoc").build());

    /*
     * Set Workflow options such as WorkflowId and Task Queue so the worker knows where to list and which workflows to execute.
     */
    WorkflowOptions options = WorkflowOptions.newBuilder()
        .setTaskQueue(Shared.HELLO_WORLD_TASK_QUEUE)
        .build();

    // Create the workflow client stub. It is used to start our workflow execution.
    GobblinTemporalWorkflow workflow = client.newWorkflowStub(GobblinTemporalWorkflow.class, options);

    /*
     * Execute our workflow and wait for it to complete. The call to our getGreeting method is
     * synchronous.
     *
     * Replace the parameter "World" in the call to getGreeting() with your name.
     */
    String greeting = workflow.getGreeting("World");

    String workflowId = WorkflowStub.fromTyped(workflow).getExecution().getWorkflowId();
    // Display workflow execution results
    LOGGER.info(workflowId + " " + greeting);
  }

  public static WorkflowServiceStubs createServiceStubs()
      throws Exception {
    GobblinClusterUtils.setSystemProperties(ConfigFactory.load());
    Config config = GobblinClusterUtils.addDynamicConfig(ConfigFactory.load());
    String SHARED_KAFKA_CONFIG_PREFIX_WITH_DOT = "gobblin.kafka.sharedConfig.";
    String SSL_KEYMANAGER_ALGORITHM = SHARED_KAFKA_CONFIG_PREFIX_WITH_DOT + "ssl.keymanager.algorithm";
    String SSL_KEYSTORE_TYPE = SHARED_KAFKA_CONFIG_PREFIX_WITH_DOT + "ssl.keystore.type";
    String SSL_KEYSTORE_LOCATION = SHARED_KAFKA_CONFIG_PREFIX_WITH_DOT + "ssl.keystore.location";
    String SSL_KEY_PASSWORD = SHARED_KAFKA_CONFIG_PREFIX_WITH_DOT + "ssl.key.password";
    String SSL_TRUSTSTORE_LOCATION = SHARED_KAFKA_CONFIG_PREFIX_WITH_DOT + "ssl.truststore.location";
    String SSL_TRUSTSTORE_PASSWORD = SHARED_KAFKA_CONFIG_PREFIX_WITH_DOT + "ssl.truststore.password";

    List<String> SSL_CONFIG_DEFAULT_SSL_PROTOCOLS = Collections.unmodifiableList(
        Arrays.asList("TLSv1.2"));
    List<String> SSL_CONFIG_DEFAULT_CIPHER_SUITES = Collections.unmodifiableList(Arrays.asList(
        // The following list is from https://github.com/netty/netty/blob/4.1/codec-http2/src/main/java/io/netty/handler/codec/http2/Http2SecurityUtil.java#L50
        "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",

        /* REQUIRED BY HTTP/2 SPEC */
        "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
        /* REQUIRED BY HTTP/2 SPEC */

        "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
        "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
        "TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256",
        "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256"
    ));

    String keyStoreType = config.getString(SSL_KEYSTORE_TYPE);
    File keyStoreFile = new File(config.getString(SSL_KEYSTORE_LOCATION));
    String keyStorePassword = config.getString(SSL_KEY_PASSWORD);

    KeyStore keyStore = KeyStore.getInstance(keyStoreType);
    keyStore.load(toInputStream(keyStoreFile), keyStorePassword.toCharArray());

    // Set key manager from key store
    String sslKeyManagerAlgorithm = config.getString(SSL_KEYMANAGER_ALGORITHM);
    KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(sslKeyManagerAlgorithm);
    keyManagerFactory.init(keyStore, keyStorePassword.toCharArray());

    // Set trust manager from trust store
    KeyStore trustStore = KeyStore.getInstance("JKS");
    File trustStoreFile = new File(config.getString(SSL_TRUSTSTORE_LOCATION));
    LOGGER.info("SSL_TRUSTSTORE_LOCATION " + config.getString(SSL_TRUSTSTORE_LOCATION));

    String trustStorePassword = config.getString(SSL_TRUSTSTORE_PASSWORD);
    trustStore.load(toInputStream(trustStoreFile), trustStorePassword.toCharArray());
    TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance("SunX509");
    trustManagerFactory.init(trustStore);

    SslContext sslContext = GrpcSslContexts.forClient()
        .keyManager(keyManagerFactory)
        .trustManager(trustManagerFactory)
        .protocols(SSL_CONFIG_DEFAULT_SSL_PROTOCOLS)
        .ciphers(SSL_CONFIG_DEFAULT_CIPHER_SUITES)
        .build();

    LOGGER.info("SSLContext: " + sslContext);

    return WorkflowServiceStubs.newServiceStubs(
        WorkflowServiceStubsOptions.newBuilder()
            .setTarget("1.nephos-temporal.corp-lca1.atd.corp.linkedin.com:7233")
            .setEnableHttps(true)
            .setSslContext(sslContext)
            .build());

  }
  /**
   * Stop the Gobblin Cluster Manager.
   */
  @Override
  public synchronized void stop() {
    if (this.stopStatus.isStopInProgress()) {
      return;
    }

    this.stopStatus.setStopInprogress(true);

    LOGGER.info("Stopping the Gobblin Cluster Manager");

    if (this.idleProcessThread != null) {
      try {
        this.idleProcessThread.join();
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
      }
    }

    stopAppLauncherAndServices();

  }

  /**
   * Get additional {@link Tag}s required for any type of reporting.
   */
  private List<? extends Tag<?>> getMetadataTags(String applicationName, String applicationId) {
    return Tag.fromMap(
        new ImmutableMap.Builder<String, Object>().put(GobblinClusterMetricTagNames.APPLICATION_NAME, applicationName)
            .put(GobblinClusterMetricTagNames.APPLICATION_ID, applicationId).build());
  }

  /**
   * Build the {@link JobConfigurationManager} for the Application Master.
   */
  private JobConfigurationManager buildJobConfigurationManager(Config config) {
    try {
      List<Object> argumentList = (this.jobCatalog != null)? ImmutableList.of(this.eventBus, config, this.jobCatalog, this.fs) :
          ImmutableList.of(this.eventBus, config, this.fs);
      if (config.hasPath(GobblinClusterConfigurationKeys.JOB_CONFIGURATION_MANAGER_KEY)) {
        return (JobConfigurationManager) GobblinConstructorUtils.invokeLongestConstructor(Class.forName(
            config.getString(GobblinClusterConfigurationKeys.JOB_CONFIGURATION_MANAGER_KEY)), argumentList.toArray(new Object[argumentList.size()]));
      } else {
        return new JobConfigurationManager(this.eventBus, config);
      }
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("unused")
  @Subscribe
  public void handleApplicationMasterShutdownRequest(ClusterManagerShutdownRequest shutdownRequest) {
    stop();
  }

  @Override
  public void close() throws IOException {
    this.applicationLauncher.close();
  }

  @Override
  public Collection<StandardMetrics> getStandardMetricsCollection() {
    List<StandardMetrics> list = new ArrayList();
    list.addAll(this.jobCatalog.getStandardMetricsCollection());
    list.addAll(this.jobConfigurationManager.getStandardMetricsCollection());
    return list;
  }

  /**
   * TODO for now the cluster id is hardcoded to 1 both here and in the {@link GobblinTaskRunner}. In the future, the
   * cluster id should be created by the {@link GobblinTemporalClusterManager} and passed to each {@link GobblinTaskRunner} via
   * Helix (at least that would be the easiest approach, there are certainly others ways to do it).
   */
  private static String getApplicationId() {
    return "1";
  }

  private static Options buildOptions() {
    Options options = new Options();
    options.addOption("a", GobblinClusterConfigurationKeys.APPLICATION_NAME_OPTION_NAME, true, "Gobblin application name");
    options.addOption("s", GobblinClusterConfigurationKeys.STANDALONE_CLUSTER_MODE, true, "Standalone cluster mode");
    options.addOption("i", GobblinClusterConfigurationKeys.HELIX_INSTANCE_NAME_OPTION_NAME, true, "Helix instance name");
    return options;
  }

  private static void printUsage(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp(GobblinTemporalClusterManager.class.getSimpleName(), options);
  }

  public static void main(String[] args) throws Exception {
    Options options = buildOptions();
    try {
      CommandLine cmd = new DefaultParser().parse(options, args);
      if (!cmd.hasOption(GobblinClusterConfigurationKeys.APPLICATION_NAME_OPTION_NAME)) {
        printUsage(options);
        System.exit(1);
      }

      boolean isStandaloneClusterManager = false;
      if (cmd.hasOption(GobblinClusterConfigurationKeys.STANDALONE_CLUSTER_MODE)) {
        isStandaloneClusterManager = Boolean.parseBoolean(cmd.getOptionValue(GobblinClusterConfigurationKeys.STANDALONE_CLUSTER_MODE, "false"));
      }

      LOGGER.info(JvmUtils.getJvmInputArguments());
      Config config = ConfigFactory.load();

      if (cmd.hasOption(GobblinClusterConfigurationKeys.HELIX_INSTANCE_NAME_OPTION_NAME)) {
        config = config.withValue(GobblinClusterConfigurationKeys.HELIX_INSTANCE_NAME_KEY,
            ConfigValueFactory.fromAnyRef(cmd.getOptionValue(
                GobblinClusterConfigurationKeys.HELIX_INSTANCE_NAME_OPTION_NAME)));
      }

      if (isStandaloneClusterManager) {
        config = config.withValue(GobblinClusterConfigurationKeys.STANDALONE_CLUSTER_MODE_KEY,
            ConfigValueFactory.fromAnyRef(true));
      }

      try (GobblinTemporalClusterManager gobblinClusterManager = new GobblinTemporalClusterManager(
          cmd.getOptionValue(GobblinClusterConfigurationKeys.APPLICATION_NAME_OPTION_NAME), getApplicationId(),
          config, Optional.<Path>absent())) {
        gobblinClusterManager.start();
      }
    } catch (ParseException pe) {
      printUsage(options);
      System.exit(1);
    }
  }

  @Override
  public void becomeActive() {
    startAppLauncherAndServices();
  }

  @Override
  public void becomeStandby() {
    stopAppLauncherAndServices();
    try {
      initializeAppLauncherAndServices();
    } catch (Exception e) {
      throw new RuntimeException("Exception reinitializing app launcher services ", e);
    }
  }

  static class StopStatus {
    @Getter
    @Setter
    AtomicBoolean isStopInProgress;
    public StopStatus(boolean inProgress) {
      isStopInProgress = new AtomicBoolean(inProgress);
    }
    public void setStopInprogress (boolean inProgress) {
      isStopInProgress.set(inProgress);
    }
    public boolean isStopInProgress () {
      return isStopInProgress.get();
    }
  }
}
