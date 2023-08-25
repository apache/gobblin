package org.apache.gobblin.runtime.mapreduce.kubernetes;

import com.google.common.base.Splitter;
import com.google.common.io.Closer;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.fsm.FiniteStateMachine;
import org.apache.gobblin.metastore.FsStateStore;
import org.apache.gobblin.metastore.StateStore;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.metrics.event.JobStateEventBuilder;
import org.apache.gobblin.runtime.AbstractJobLauncher;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.runtime.TaskState;
import org.apache.gobblin.runtime.TaskStateCollectorService;
import org.apache.gobblin.runtime.job.GobblinJobFiniteStateMachine;
import org.apache.gobblin.runtime.job.GobblinJobFiniteStateMachine.JobFSMState;
import org.apache.gobblin.runtime.job.GobblinJobFiniteStateMachine.StateType;
import org.apache.gobblin.source.workunit.MultiWorkUnit;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.util.JobConfigurationUtils;
import org.apache.gobblin.util.JobLauncherUtils;
import org.apache.gobblin.util.ParallelRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class KuberJobLauncher extends AbstractJobLauncher {

    private static final Logger log = LoggerFactory.getLogger(KuberJobLauncher.class);
    private static final Splitter SPLITTER = Splitter.on(',').omitEmptyStrings().trimResults();

    private static final String MAGIC_KUBER_VOLUME = "/a";
    private static final String MAGIC_KUBERNETES_PATH = MAGIC_KUBER_VOLUME + "/gobblin-kuber";

    private final Configuration conf;
    private final FileSystem fs;
    private final KJob job;
    private final Path mrJobDir;
    private final Path jarsDir;
    private final Path unsharedJarsDir;
    private final Path jobInputPath;
    private final TaskStateCollectorService taskStateCollectorService;
    private final GobblinJobFiniteStateMachine fsm;

    private volatile boolean kuberJobSubmitted = false;

    public KuberJobLauncher(final Properties jobProps,
                            final Configuration conf,
                            final List<? extends Tag<?>> metadataTags) throws Exception {
        super(jobProps, metadataTags);

        this.conf = conf;

        this.fsm = GobblinJobFiniteStateMachine
                .builder()
                .jobState(jobContext.getJobState())
                .interruptGracefully(this::killJob)
                .killJob(this::killJob)
                .build();

        JobConfigurationUtils.putPropertiesIntoConfiguration(this.jobProps, conf);

        this.fs = FileSystem.newInstance(URI.create(ConfigurationKeys.LOCAL_FS_URI), conf);

        this.mrJobDir = new Path(
                new Path(MAGIC_KUBERNETES_PATH, this.jobContext.getJobName()),
                this.jobContext.getJobId()
        );
        if (this.fs.exists(this.mrJobDir)) {
            log.warn("Job working directory already exists for job {}", this.jobContext.getJobName());
            this.fs.delete(this.mrJobDir, true);
        }
        this.fs.mkdirs(this.mrJobDir);

        this.unsharedJarsDir = new Path(this.mrJobDir, "_jars");

        this.jarsDir = new Path(this.mrJobDir, "jars");
        this.jobInputPath = new Path(this.mrJobDir, "input");
        final Path jobOutputPath = new Path(this.mrJobDir, "output");
        final Path outputTaskStateDir = new Path(jobOutputPath, this.jobContext.getJobId());

        this.job = KJob.getInstance("Gobblin-" + this.jobContext.getJobName());

        // StateStore interface uses the following key (rootDir, storeName, tableName)
        // The state store base is the root directory and the last two elements of the path are used as the storeName and
        // tableName. Create the state store with the root at jobOutputPath. The task state will be stored at
        // jobOutputPath/output/taskState.tst, so output will be the storeName.
        final StateStore<TaskState> taskStateStore = new FsStateStore<>(this.fs, jobOutputPath.toString(), TaskState.class);

        this.taskStateCollectorService = new TaskStateCollectorService(
                jobProps,
                this.jobContext.getJobState(),
                this.eventBus,
                taskStateStore,
                outputTaskStateDir
        );

        log.info("Configured fs: {}", fs);
        log.debug("Configuration: {}", conf);
        startCancellationExecutor();
    }

    @Override
    public void close() throws IOException {
        try {
            if (this.kuberJobSubmitted && !this.job.isComplete()) {
                log.info("Killing the Kuber job for job {}", this.jobContext.getJobId());
                this.job.kill();
            }
        }
        finally {
            super.close();
            fs.close();
        }
    }

    @Override
    protected void executeCancellation() {
        try (final FiniteStateMachine<JobFSMState>.Transition transition = this.fsm.startTransition(this.fsm.getEndStateForType(StateType.CANCELLED))) {
            if (transition.getStartState().getStateType().equals(StateType.RUNNING)) {
                try {
                    this.killJob();
                }
                catch (final IOException ioe) {
                    log.error("Failed to kill the Kuber job for job {}", this.jobContext.getJobId());
                    transition.changeEndState(this.fsm.getEndStateForType(StateType.FAILED));
                }
            }
        }
        catch (final GobblinJobFiniteStateMachine.FailedTransitionCallbackException exc) {
            exc.getTransition().switchEndStateToErrorState();
            exc.getTransition().closeWithoutCallbacks();
        }
        catch (final FiniteStateMachine.UnallowedTransitionException | InterruptedException exc) {
            log.error("Failed to cancel job {}", this.jobContext.getJobId(), exc);
        }
    }

    @Override
    protected void runWorkUnits(final List<WorkUnit> workUnits) throws IOException {
        try {
            this.prepareKuberJob(workUnits);

            // Start the output TaskState collector service
            this.taskStateCollectorService.startAsync().awaitRunning();

            log.info("Launching Kuber job {}", this.job.getJobName());
            try (final FiniteStateMachine<JobFSMState>.Transition t = this.fsm.startTransition(this.fsm.getEndStateForType(StateType.RUNNING))) {
                try {
                    final String location = this.mrJobDir.toString().substring(MAGIC_KUBER_VOLUME.length() + 1);
                    this.job.submit(location);
                }
                catch (final Throwable exc) {
                    t.changeEndState(this.fsm.getEndStateForType(StateType.FAILED));
                    throw exc;
                }
                this.kuberJobSubmitted = true;
            }
            catch (final FiniteStateMachine.UnallowedTransitionException unallowed) {
                log.error("Cannot start Kuber job.", unallowed);
            }

            if (this.fsm.getCurrentState().getStateType().equals(StateType.RUNNING)) {
                log.info("Waiting for Kuber job {} to complete", this.job);
                this.job.waitForCompletion();
                this.fsm.transitionIfAllowed(fsm.getEndStateForType(StateType.SUCCESS));
            }
        }
        catch (final Throwable t) {
            t.printStackTrace();
            throw new RuntimeException("The Kuber job cannot be submitted", t);
        }
        finally {
            final JobStateEventBuilder eventBuilder = new JobStateEventBuilder(JobStateEventBuilder.MRJobState.MR_JOB_STATE);

            if (!kuberJobSubmitted) {
                eventBuilder.jobTrackingURL = "";
                eventBuilder.status = JobStateEventBuilder.Status.FAILED;
            }
            else {
                eventBuilder.jobTrackingURL = "";
                eventBuilder.status = JobStateEventBuilder.Status.SUCCEEDED;
                if (!this.job.isSuccessful())
                    eventBuilder.status = JobStateEventBuilder.Status.FAILED;
            }
            this.eventSubmitter.submit(eventBuilder);

            // The last iteration of output TaskState collecting will run when the collector service gets stopped
            this.taskStateCollectorService.stopAsync().awaitTerminated();
        }
    }


    private void killJob() throws IOException {
        log.info("Killing the Kuber job for job {}", this.jobContext.getJobId());
        this.job.kill();
        // Collect final task states.
        this.taskStateCollectorService.stopAsync().awaitTerminated();
    }


    private void prepareKuberJob(final List<WorkUnit> workUnits) throws IOException {
        addDependencies(this.conf);

        prepareJobInput(workUnits);

        serializeJobState(this.fs, this.mrJobDir, this.jobContext.getJobState());

        final Path confTargetPath = new Path(this.mrJobDir, "conf.xml");
        fs.delete(confTargetPath, false);
        try (final DataOutputStream dos = new DataOutputStream(fs.create(confTargetPath))) {
            this.conf.writeXml(dos);
        }
    }

    private void addDependencies(final Configuration conf) throws IOException {
        if (this.jobProps.containsKey(ConfigurationKeys.FRAMEWORK_JAR_FILES_KEY))
            addJars(this.jarsDir, this.jobProps.getProperty(ConfigurationKeys.FRAMEWORK_JAR_FILES_KEY), conf);

        if (this.jobProps.containsKey(ConfigurationKeys.JOB_JAR_FILES_KEY))
            addJars(this.jarsDir, this.jobProps.getProperty(ConfigurationKeys.JOB_JAR_FILES_KEY), conf);

        if (this.jobProps.containsKey(ConfigurationKeys.JOB_LOCAL_FILES_KEY))
            for (final String jobFile : SPLITTER.split(this.jobProps.getProperty(ConfigurationKeys.JOB_LOCAL_FILES_KEY)))
                this.fs.copyFromLocalFile(
                        new Path(jobFile),
                        new Path(this.fs.makeQualified(new Path(this.mrJobDir, "files")), new Path(jobFile).getName())
                );

        final List<String> jars = Arrays
                .stream(((URLClassLoader) ClassLoader.getSystemClassLoader()).getURLs())
                .map(URL::getFile)
                .filter(it -> it.toLowerCase().endsWith(".jar"))
                .collect(Collectors.toList());
        for (final String jar : jars)
            addJars(this.jarsDir, jar, conf);
    }

    void serializeJobState(final FileSystem fs,
                           final Path mrJobDir,
                           final JobState jobState) throws IOException {
        final Path jobStateFilePath = new Path(mrJobDir, JOB_STATE_FILE_NAME);
        // Write the job state with an empty task set (work units are read by the runner from a different file)
        try (final DataOutputStream dataOutputStream = new DataOutputStream(fs.create(jobStateFilePath))) {
            jobState.write(dataOutputStream, false, true);
        }

        this.conf.set(ConfigurationKeys.JOB_STATE_FILE_PATH_KEY, jobStateFilePath.toString());
        this.conf.set(ConfigurationKeys.JOB_STATE_DISTRIBUTED_CACHE_NAME, jobStateFilePath.getName());
    }

    private void addJars(final Path jarFileDir,
                         final String jarFileList,
                         final Configuration conf) throws IOException {
        final LocalFileSystem lfs = FileSystem.getLocal(conf);
        for (final String jarFile : SPLITTER.split(jarFileList)) {
            final Path srcJarFile = new Path(jarFile);
            final FileStatus[] fileStatusList = lfs.globStatus(srcJarFile);

            for (final FileStatus status : fileStatusList) {
                // For each FileStatus there are chances it could fail in copying at the first attempt, due to file-existence
                // or file-copy is ongoing by other job instance since all Gobblin jobs share the same jar file directory.
                // the retryCount is to avoid cases (if any) where retry is going too far and causes job hanging.

                // SNAPSHOT jars should not be shared, as different jobs may be using different versions of it
                final Path baseDir = status.getPath().getName().contains("SNAPSHOT") ? this.unsharedJarsDir : jarFileDir;
                final Path destJarFile = new Path(this.fs.makeQualified(baseDir), status.getPath().getName());

                // Adding destJarFile into HDFS until it exists and the size of file on targetPath matches the one on local path.
                while (!this.fs.exists(destJarFile) || fs.getFileStatus(destJarFile).getLen() != status.getLen()) {
                    try {
                        if (this.fs.exists(destJarFile) && fs.getFileStatus(destJarFile).getLen() != status.getLen()) {
                            Thread.sleep(3000L);
                            throw new IOException("Waiting for file to complete on uploading ... ");
                        }
                        // Set the first parameter as false for not deleting sourceFile
                        // Set the second parameter as false for not overwriting existing file on the target, by default it is true.
                        // If the file is preExisted but overwrite flag set to false, then an IOException if thrown.
                        this.fs.copyFromLocalFile(false, false, status.getPath(), destJarFile);
                    }
                    catch (final IOException | InterruptedException e) {
                        log.warn("Path:{} is not copied successfully. Will require retry.", destJarFile);
                        log.error("The jar file:{} failed in being copied into hdfs", destJarFile, e);
                        // If retry reaches upper limit, skip copying this file.
                        break;
                    }
                }
            }
        }
    }

    private void prepareJobInput(final List<WorkUnit> workUnits) throws IOException {
        final Closer closer = Closer.create();
        try {
            final ParallelRunner parallelRunner = closer.register(new ParallelRunner(
                    ParallelRunner.DEFAULT_PARALLEL_RUNNER_THREADS, this.fs));

            int multiTaskIdSequence = 0;
            // Serialize each work unit into a file named after the task ID
            for (final WorkUnit workUnit : workUnits) {
                final String workUnitFileName = workUnit instanceof MultiWorkUnit
                        ? JobLauncherUtils.newMultiTaskId(this.jobContext.getJobId(), multiTaskIdSequence++) + MULTI_WORK_UNIT_FILE_EXTENSION
                        : workUnit.getProp(ConfigurationKeys.TASK_ID_KEY) + WORK_UNIT_FILE_EXTENSION;

                final Path workUnitFile = new Path(this.jobInputPath, workUnitFileName);
                log.debug("Writing work unit file {}", workUnitFileName);

                parallelRunner.serializeToFile(workUnit, workUnitFile);
            }
        }
        catch (final Throwable t) {
            throw closer.rethrow(t);
        }
        finally {
            closer.close();
        }
    }

}
