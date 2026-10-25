package org.apache.gobblin.runtime.mapreduce.kubernetes;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ServiceManager;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;
import org.apache.gobblin.broker.SharedResourcesBrokerFactory;
import org.apache.gobblin.broker.gobblin_scopes.GobblinScopeTypes;
import org.apache.gobblin.broker.gobblin_scopes.JobScopeInstance;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;
import org.apache.gobblin.commit.CommitStep;
import org.apache.gobblin.configuration.DynamicConfigGenerator;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.metastore.FsStateStore;
import org.apache.gobblin.metastore.StateStore;
import org.apache.gobblin.runtime.AbstractJobLauncher;
import org.apache.gobblin.runtime.DynamicConfigGeneratorFactory;
import org.apache.gobblin.runtime.GobblinMultiTaskAttempt;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.runtime.TaskExecutor;
import org.apache.gobblin.runtime.TaskState;
import org.apache.gobblin.runtime.TaskStateTracker;
import org.apache.gobblin.source.workunit.MultiWorkUnit;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.HadoopUtils;
import org.apache.gobblin.util.JobLauncherUtils;
import org.apache.gobblin.util.SerializationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class KubeTaskRunner {

    private static final Logger log = LoggerFactory.getLogger(KubeTaskRunner.class);

    private static final Joiner JOINER = Joiner.on('/');

    private final String stuffLocation;
    private final String wusLocation;
    private final String outputLocation;

    private FileSystem fs;
    private StateStore<TaskState> taskStateStore;
    private TaskExecutor taskExecutor;
    private TaskStateTracker taskStateTracker;
    private ServiceManager serviceManager;

    private final Configuration conf;
    private final JobState jobState = new JobState();
    private final List<WorkUnit> workUnits = Lists.newArrayList();

    public KubeTaskRunner(final String stuffLocation) {
        this.stuffLocation = stuffLocation;
        this.conf = new Configuration(false);
        this.conf.addResource(JOINER.join(stuffLocation, "conf.xml"));

        this.wusLocation = JOINER.join(this.stuffLocation, "input");
        this.outputLocation = JOINER.join(this.stuffLocation, "output");
    }

    public void run() throws IOException, InterruptedException {
        setup(conf);
        try {
            final LocatedFileStatus next = this.fs.listFiles(new Path(wusLocation), false).next();
            final String wuLocation = next.getPath().toString();
            final WorkUnit workUnit = wuLocation.endsWith(AbstractJobLauncher.MULTI_WORK_UNIT_FILE_EXTENSION)
                    ? MultiWorkUnit.createEmpty()
                    : WorkUnit.createEmpty();
            SerializationUtils.deserializeState(this.fs, new Path(wuLocation), workUnit);
            if (workUnit instanceof MultiWorkUnit) {
                final List<WorkUnit> flattenedWorkUnits = JobLauncherUtils.flattenWorkUnits(((MultiWorkUnit) workUnit).getWorkUnits());
                workUnits.addAll(flattenedWorkUnits);
            }
            else {
                workUnits.add(workUnit);
            }

            try (final FileInputStream fis = new FileInputStream(JOINER.join(this.stuffLocation, "/job.state"))) {
                SerializationUtils.deserializeStateFromInputStream(fis, this.jobState);
            }

            @SuppressWarnings("ConstantConditions")
            final SharedResourcesBroker<GobblinScopeTypes> globalBroker =
                    SharedResourcesBrokerFactory.createDefaultTopLevelBroker(
                            ConfigFactory.parseProperties(jobState.getProperties()),
                            GobblinScopeTypes.GLOBAL.defaultScopeInstance());

            final SharedResourcesBroker<GobblinScopeTypes> jobBroker = globalBroker
                    .newSubscopedBuilder(new JobScopeInstance(jobState.getJobName(), jobState.getJobId()))
                    .build();

            GobblinMultiTaskAttempt.runWorkUnits(
                    jobState.getJobId(),
                    "dummyContainerId",
                    jobState,
                    workUnits,
                    taskStateTracker,
                    taskExecutor,
                    taskStateStore,
                    GobblinMultiTaskAttempt.CommitPolicy.IMMEDIATE,
                    jobBroker,
                    gmta -> false
            );
        }
        finally {
            final CommitStep cleanUpCommitStep = new CommitStep() {
                @Override
                public boolean isCompleted() {
                    return !serviceManager.isHealthy();
                }

                @Override
                public void execute() {
                    log.info("Starting the clean-up steps.");
                    try {
                        serviceManager.stopAsync().awaitStopped(5, TimeUnit.SECONDS);
                    }
                    catch (final TimeoutException te) {
                        // Ignored
                    }
                }
            };
            cleanUpCommitStep.execute();
        }
    }

    private void setup(final Configuration conf) throws IOException {
        final State gobblinJobState = HadoopUtils.getStateFromConf(conf);

        try (final Closer closer = Closer.create()) {
            this.fs = FileSystem.get(conf);

            this.taskStateStore = new FsStateStore<>(
                    fs,
                    new Path(this.outputLocation).toUri().getPath(),
                    TaskState.class
            );

            final Path jobStateFile = new Path(JOINER.join(stuffLocation, "job.state"));
            final FileInputStream readFrom = closer.register(new FileInputStream(jobStateFile.toUri().getPath()));
            SerializationUtils.deserializeStateFromInputStream(readFrom, jobState);
        }

        // load dynamic configuration to add to the job configuration
        final Config jobStateAsConfig = ConfigUtils.propertiesToConfig(jobState.getProperties());
        final DynamicConfigGenerator dynamicConfigGenerator = DynamicConfigGeneratorFactory
                .createDynamicConfigGenerator(jobStateAsConfig);
        final Config dynamicConfig = dynamicConfigGenerator.generateDynamicConfig(jobStateAsConfig);

        for (final Map.Entry<String, ConfigValue> entry : dynamicConfig.entrySet()) {
            jobState.setProp(entry.getKey(), entry.getValue().unwrapped().toString());
            conf.set(entry.getKey(), entry.getValue().unwrapped().toString());
            gobblinJobState.setProp(entry.getKey(), entry.getValue().unwrapped().toString());
        }

        this.taskExecutor = new TaskExecutor(conf);
        this.taskStateTracker = new KuberTaskStateTracker(conf, log);
        this.serviceManager = new ServiceManager(Lists.newArrayList(taskExecutor, taskStateTracker));
        try {
            this.serviceManager.startAsync().awaitHealthy(5, TimeUnit.SECONDS);
        }
        catch (final TimeoutException te) {
            log.error("Timed out while waiting for the service manager to start up", te);
            throw new RuntimeException(te);
        }

        AbstractJobLauncher.setDefaultAuthenticator(jobState.getProperties());
    }


    public static void main(final String... args) throws Exception {
        if (Objects.requireNonNull(args, "args").length != 1)
            throw new IllegalArgumentException("expecting exactly 1 argument, got=" + args.length);

        final String stuffLocation = args[0];
        new KubeTaskRunner(stuffLocation).run();
    }

}
