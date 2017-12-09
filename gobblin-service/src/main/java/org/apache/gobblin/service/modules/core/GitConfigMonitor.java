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
package org.apache.gobblin.service.modules.core;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.gobblin.runtime.api.SpecNotFoundException;
import org.apache.gobblin.runtime.spec_store.FSSpecStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.ResetCommand;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.diff.DiffEntry;
import org.eclipse.jgit.errors.RepositoryNotFoundException;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectReader;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.treewalk.CanonicalTreeParser;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import com.google.common.util.concurrent.AbstractIdleService;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.config.ConfigBuilder;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.spec_catalog.FlowCatalog;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.ExecutorsUtils;
import org.apache.gobblin.util.PullFileLoader;

import lombok.extern.slf4j.Slf4j;


/**
 * Service that monitors for jobs from a git repository.
 * The git repository must have an inital commit that has no config files since that is used as a base for getting
 * the change list.
 * The config needs to be organized with the following structure:
 * <root_config_dir>/<flowGroup>/<flowName>.(pull|job|json|conf)
 * The <flowGroup> and <flowName> is used to generate the URI used to store the config in the {@link FlowCatalog}
 */
@Slf4j
public class GitConfigMonitor extends AbstractIdleService {
  private static final String SPEC_DESCRIPTION = "Git-based flow config";
  private static final String SPEC_VERSION = FlowSpec.Builder.DEFAULT_VERSION;
  private static final int TERMINATION_TIMEOUT = 30;
  private static final int CONFIG_FILE_DEPTH = 3;
  private static final String REMOTE_NAME = "origin";

  private final ScheduledExecutorService scheduledExecutor;
  private final GitRepository gitRepo;
  private final int pollingInterval;
  private final String repositoryDir;
  private final String configDir;
  private final Path configDirPath;
  private final FlowCatalog flowCatalog;
  private final PullFileLoader pullFileLoader;
  private final Config emptyConfig = ConfigFactory.empty();
  private volatile boolean isActive = false;

  /**
   * Create a {@link GitConfigMonitor} that monitors a git repository for changes and manages config in a
   * {@link FlowCatalog}
   * @param config configuration
   * @param flowCatalog the flow catalog
   */
  GitConfigMonitor(Config config, FlowCatalog flowCatalog) {
    this.flowCatalog = flowCatalog;

    this.scheduledExecutor = Executors.newSingleThreadScheduledExecutor(
        ExecutorsUtils.newThreadFactory(Optional.of(log), Optional.of("FetchGitConfExecutor")));

    Preconditions.checkArgument(config.hasPath(ConfigurationKeys.GIT_CONFIG_MONITOR_REPO_URI),
        ConfigurationKeys.GIT_CONFIG_MONITOR_REPO_URI + " needs to be specified.");

    String repositoryUri = config.getString(ConfigurationKeys.GIT_CONFIG_MONITOR_REPO_URI);

    this.repositoryDir = ConfigUtils.getString(config, ConfigurationKeys.GIT_CONFIG_MONITOR_REPO_DIR,
        ConfigurationKeys.DEFAULT_GIT_CONFIG_MONITOR_REPO_DIR);

    this.configDir = ConfigUtils.getString(config, ConfigurationKeys.GIT_CONFIG_MONITOR_CONFIG_DIR,
        ConfigurationKeys.DEFAULT_GIT_CONFIG_MONITOR_CONFIG_DIR);

    this.pollingInterval = ConfigUtils.getInt(config, ConfigurationKeys.GIT_CONFIG_MONITOR_POLLING_INTERVAL,
        ConfigurationKeys.DEFAULT_GIT_CONFIG_MONITOR_POLLING_INTERVAL);

    String branchName = ConfigUtils.getString(config, ConfigurationKeys.GIT_CONFIG_MONITOR_BRANCH_NAME,
        ConfigurationKeys.DEFAULT_GIT_CONFIG_MONITOR_BRANCH_NAME);

    this.configDirPath = new Path(this.repositoryDir, this.configDir);

    try {
      this.pullFileLoader = new PullFileLoader(this.configDirPath,
          FileSystem.get(URI.create(ConfigurationKeys.LOCAL_FS_URI), new Configuration()),
          PullFileLoader.DEFAULT_JAVA_PROPS_PULL_FILE_EXTENSIONS, PullFileLoader.DEFAULT_HOCON_PULL_FILE_EXTENSIONS);
    } catch (IOException e) {
      throw new RuntimeException("Could not create pull file loader", e);
    }

    try {
      this.gitRepo = new GitRepository(repositoryUri, this.repositoryDir, branchName);
    } catch (GitAPIException | IOException e) {
      throw new RuntimeException("Could not open git repository", e);
    }
  }

  /** Start the service. */
  @Override
  protected void startUp() throws Exception {
    log.info("Starting the " + GitConfigMonitor.class.getSimpleName());
    log.info("Polling git with inteval {} ", this.pollingInterval);

    // Schedule the job config fetch task
    this.scheduledExecutor.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        try {
          processGitConfigChanges();
        } catch (GitAPIException | IOException e) {
          log.error("Failed to process git config changes", e);
          // next run will try again since errors could be intermittent
        }
      }
    }, 0, this.pollingInterval, TimeUnit.SECONDS);
  }

  /** Stop the service. */
  @Override
  protected void shutDown() throws Exception {
    this.scheduledExecutor.shutdown();
    this.scheduledExecutor.awaitTermination(TERMINATION_TIMEOUT, TimeUnit.SECONDS);
  }

  public synchronized void setActive(boolean isActive) {
    if (this.isActive == isActive) {
      // No-op if already in correct state
      return;
    }

    this.isActive = isActive;
  }

  /**
   * Fetch the list of changes since the last refresh of the repository and apply the changes to the {@link FlowCatalog}
   * @throws GitAPIException
   * @throws IOException
   */
  @VisibleForTesting
  void processGitConfigChanges() throws GitAPIException, IOException {
    // if not active or if the flow catalog is not up yet then can't process config changes
    if (!isActive || !this.flowCatalog.isRunning()) {
      log.info("GitConfigMonitor: skip poll since the JobCatalog is not yet running.");
      return;
    }

    List<DiffEntry> changes = this.gitRepo.getChanges();

    for (DiffEntry change : changes) {
      switch (change.getChangeType()) {
        case ADD:
        case MODIFY:
          addSpec(change);
          break;
        case DELETE:
          removeSpec(change);
          break;
        case RENAME:
          removeSpec(change);
          addSpec(change);
          break;
        default:
          throw new RuntimeException("Unsupported change type " + change.getChangeType());
      }
    }

    // Done processing changes, so checkpoint
    this.gitRepo.moveCheckpointAndHashesForward();
  }

  /**
   * Add a {@link FlowSpec} for an added, updated, or modified flow config
   * @param change
   */
  private void addSpec(DiffEntry change) {
    if (checkConfigFilePath(change.getNewPath())) {
      Path configFilePath = new Path(this.repositoryDir, change.getNewPath());

      try {
        Config flowConfig = loadConfigFileWithFlowNameOverrides(configFilePath);

        this.flowCatalog.put(FlowSpec.builder()
            .withConfig(flowConfig)
            .withVersion(SPEC_VERSION)
            .withDescription(SPEC_DESCRIPTION)
            .build());
      } catch (IOException e) {
        log.warn("Could not load config file: " + configFilePath);
      }
    }
  }

  /**
   * remove a {@link FlowSpec} for a deleted or renamed flow config
   * @param change
   */
  private void removeSpec(DiffEntry change) {
    if (checkConfigFilePath(change.getOldPath())) {
      Path configFilePath = new Path(this.repositoryDir, change.getOldPath());
      String flowName = FSSpecStore.getSpecName(configFilePath);
      String flowGroup = FSSpecStore.getSpecGroup(configFilePath);

      // build a dummy config to get the proper URI for delete
      Config dummyConfig = ConfigBuilder.create()
          .addPrimitive(ConfigurationKeys.FLOW_GROUP_KEY, flowGroup)
          .addPrimitive(ConfigurationKeys.FLOW_NAME_KEY, flowName)
          .build();

      FlowSpec spec = FlowSpec.builder()
          .withConfig(dummyConfig)
          .withVersion(SPEC_VERSION)
          .withDescription(SPEC_DESCRIPTION)
          .build();

        this.flowCatalog.remove(spec.getUri());
    }
  }


    /**
     * check whether the file has the proper naming and hierarchy
     * @param configFilePath the relative path from the repo root
     * @return false if the file does not conform
     */
  private boolean checkConfigFilePath(String configFilePath) {
    // The config needs to stored at configDir/flowGroup/flowName.(pull|job|json|conf)
    Path configFile = new Path(configFilePath);
    String fileExtension = Files.getFileExtension(configFile.getName());

    if (configFile.depth() != CONFIG_FILE_DEPTH ||
        !configFile.getParent().getParent().getName().equals(configDir) ||
        !(PullFileLoader.DEFAULT_JAVA_PROPS_PULL_FILE_EXTENSIONS.contains(fileExtension) ||
            PullFileLoader.DEFAULT_JAVA_PROPS_PULL_FILE_EXTENSIONS.contains(fileExtension))) {
      log.warn("Changed file does not conform to directory structure and file name format, skipping: "
          + configFilePath);

      return false;
    }

    return true;
  }

  /**
   * Load the config file and override the flow name and flow path properties with the names from the file path
   * @param configFilePath path of the config file relative to the repository root
   * @return the configuration object
   * @throws IOException
   */
  private Config loadConfigFileWithFlowNameOverrides(Path configFilePath) throws IOException {
    Config flowConfig = this.pullFileLoader.loadPullFile(configFilePath, emptyConfig, false);
    String flowName = FSSpecStore.getSpecName(configFilePath);
    String flowGroup = FSSpecStore.getSpecGroup(configFilePath);

    return flowConfig.withValue(ConfigurationKeys.FLOW_NAME_KEY, ConfigValueFactory.fromAnyRef(flowName))
        .withValue(ConfigurationKeys.FLOW_GROUP_KEY, ConfigValueFactory.fromAnyRef(flowGroup));
  }

  /**
   * Class for managing a git repository
   */
  private static class GitRepository {
    private final static String CHECKPOINT_FILE = "checkpoint.txt";
    private final static String CHECKPOINT_FILE_TMP = "checkpoint.tmp";
    private final String repoUri;
    private final String repoDir;
    private final String branchName;
    private Git git;
    private String lastProcessedGitHash;
    private String latestGitHash;

    /**
     * Create an object to manage the git repository stored locally at repoDir with a repository URI of repoDir
     * @param repoUri URI of repository
     * @param repoDir Directory to hold the local copy of the repository
     * @param branchName Branch name
     * @throws GitAPIException
     * @throws IOException
     */
    private GitRepository(String repoUri, String repoDir, String branchName) throws GitAPIException, IOException {
      this.repoUri = repoUri;
      this.repoDir = repoDir;
      this.branchName = branchName;

      initRepository();
    }

    /**
     * Open the repository if it exists locally, otherwise clone it
     * @throws GitAPIException
     * @throws IOException
     */
    private void initRepository() throws GitAPIException, IOException {
      File repoDirFile = new File(this.repoDir);

      try {
        this.git = Git.open(repoDirFile);

        String uri = this.git.getRepository().getConfig().getString("remote", REMOTE_NAME, "url");

        if (!uri.equals(this.repoUri)) {
          throw new RuntimeException("Repo at " + this.repoDir + " has uri " + uri + " instead of " + this.repoUri);
        }
      } catch (RepositoryNotFoundException e) {
        // if the repository was not found then clone a new one
        this.git = Git.cloneRepository()
            .setDirectory(repoDirFile)
            .setURI(this.repoUri)
            .setBranch(this.branchName)
            .call();
      }

      try {
        this.lastProcessedGitHash = readCheckpoint();
      } catch (FileNotFoundException e) {
        // if no checkpoint is available then start with the first commit
        Iterable<RevCommit> logs = git.log().call();
        RevCommit lastLog = null;

        for (RevCommit log : logs) {
          lastLog = log;
        }

        if (lastLog != null) {
          this.lastProcessedGitHash = lastLog.getName();
        }
      }

      this.latestGitHash = this.lastProcessedGitHash;
    }

    /**
     * Read the last processed commit githash from the checkpoint file
     * @return
     * @throws IOException
     */
    private String readCheckpoint() throws IOException {
      File checkpointFile = new File(this.repoDir, CHECKPOINT_FILE);
      return Files.toString(checkpointFile, Charsets.UTF_8);
    }

    /**
     * Write the last processed commit githash to the checkpoint file
     * @param gitHash
     * @throws IOException
     */
    private void writeCheckpoint(String gitHash) throws IOException {
      // write to a temporary name then rename to make the operation atomic when the file system allows a file to be
      // replaced
      File tmpCheckpointFile = new File(this.repoDir, CHECKPOINT_FILE_TMP);
      File checkpointFile = new File(this.repoDir, CHECKPOINT_FILE);

      Files.write(gitHash, tmpCheckpointFile, Charsets.UTF_8);

      Files.move(tmpCheckpointFile, checkpointFile);
    }

    private void moveCheckpointAndHashesForward() throws IOException {
      this.lastProcessedGitHash = this.latestGitHash;

      writeCheckpoint(this.latestGitHash);
    }

    /**
     *
     * @throws GitAPIException
     * @throws IOException
     */
    private List<DiffEntry> getChanges() throws GitAPIException, IOException {
      // get tree for last processed commit
      ObjectId oldHeadTree = git.getRepository().resolve(this.lastProcessedGitHash + "^{tree}");

      // refresh to latest and reset hard to handle forced pushes
      this.git.fetch().setRemote(REMOTE_NAME).call();
      // reset hard to get a clean working set since pull --rebase may leave files around
      this.git.reset().setMode(ResetCommand.ResetType.HARD).setRef(REMOTE_NAME + "/" + this.branchName).call();

      ObjectId head = this.git.getRepository().resolve("HEAD");
      ObjectId headTree = this.git.getRepository().resolve("HEAD^{tree}");

      // remember the hash for the current HEAD. This will be checkpointed after the diff is processed.
      latestGitHash = head.getName();

      // diff old and new heads to find changes
      ObjectReader reader = this.git.getRepository().newObjectReader();
      CanonicalTreeParser oldTreeIter = new CanonicalTreeParser();
      oldTreeIter.reset(reader, oldHeadTree);
      CanonicalTreeParser newTreeIter = new CanonicalTreeParser();
      newTreeIter.reset(reader, headTree);

      return this.git.diff()
          .setNewTree(newTreeIter)
          .setOldTree(oldTreeIter)
          .setShowNameAndStatusOnly(true)
          .call();
    }
  }
}
