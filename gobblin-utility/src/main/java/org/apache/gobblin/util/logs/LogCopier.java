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

package org.apache.gobblin.util.logs;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.io.Closer;
import com.google.common.io.Files;
import com.google.common.util.concurrent.AbstractScheduledService;

import lombok.Getter;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.util.DatasetFilterUtils;
import org.apache.gobblin.util.FileListUtils;


/**
 * A utility class that periodically reads log files in a source log file directory for changes
 * since the last reads and appends the changes to destination log files with the same names as
 * the source log files in a destination log directory. The source and destination log files
 * can be on different {@link FileSystem}s.
 *
 * <p>
 *   This class extends the {@link AbstractScheduledService} so it can be used with a
 *   {@link com.google.common.util.concurrent.ServiceManager} that manages the lifecycle of
 *   a {@link LogCopier}.
 * </p>
 *
 * <p>
 *   This class is intended to be used in the following pattern:
 *
 *   <pre>
 *     {@code
 *       LogCopier.Builder logCopierBuilder = LogCopier.newBuilder();
 *       LogCopier logCopier = logCopierBuilder
 *           .useSrcFileSystem(FileSystem.getLocal(new Configuration()))
 *           .useDestFileSystem(FileSystem.get(URI.create(destFsUri), new Configuration()))
 *           .readFrom(new Path(srcLogDir))
 *           .writeTo(new Path(destLogDir))
 *           .useSourceLogFileMonitorInterval(60)
 *           .useTimeUnit(TimeUnit.SECONDS)
 *           .build();
 *
 *       ServiceManager serviceManager = new ServiceManager(Lists.newArrayList(logCopier));
 *       serviceManager.startAsync();
 *
 *       // ...
 *       serviceManager.stopAsync().awaitStopped(60, TimeUnit.SECONDS);
 *     }
 *   </pre>
 *
 *   Checkout the Javadoc of {@link LogCopier.Builder} to see the available options for customization.
 * </p>
 *
 * @author Yinan Li
 */
public class LogCopier extends AbstractScheduledService {

  private static final Logger LOGGER = LoggerFactory.getLogger(LogCopier.class);

  private static final long DEFAULT_SOURCE_LOG_FILE_MONITOR_INTERVAL = 120;
  private static final TimeUnit DEFAULT_TIME_UNIT = TimeUnit.SECONDS;
  private static final int DEFAULT_LINES_WRITTEN_BEFORE_FLUSH = 100;

  private static final int DEFAULT_NUM_COPY_THREADS = 10;

  private final FileSystem srcFs;
  private final FileSystem destFs;
  private final List<Path> srcLogDirs;
  private final Path destLogDir;

  private final long sourceLogFileMonitorInterval;
  private final TimeUnit timeUnit;

  private final Set<String> logFileExtensions;
  private final int numCopyThreads;
  private final String currentLogFileName;

  private final Optional<List<Pattern>> includingRegexPatterns;
  private final Optional<List<Pattern>> excludingRegexPatterns;

  private final Optional<String> logFileNamePrefix;

  private final int linesWrittenBeforeFlush;

  private final ExecutorService executorService;

  @Getter
  private final Set<String> copiedFileNames = Sets.newConcurrentHashSet();
  private boolean shouldCopyCurrentLogFile;

  private LogCopier(Builder builder) {
    this.srcFs = builder.srcFs;
    this.destFs = builder.destFs;

    this.srcLogDirs = builder.srcLogDirs.stream().map(d -> this.srcFs.makeQualified(d)).collect(Collectors.toList());
    this.destLogDir = this.destFs.makeQualified(builder.destLogDir);

    this.sourceLogFileMonitorInterval = builder.sourceLogFileMonitorInterval;
    this.timeUnit = builder.timeUnit;

    this.logFileExtensions = builder.logFileExtensions;
    this.currentLogFileName = builder.currentLogFileName;
    this.shouldCopyCurrentLogFile = false;

    this.includingRegexPatterns = Optional.fromNullable(builder.includingRegexPatterns);
    this.excludingRegexPatterns = Optional.fromNullable(builder.excludingRegexPatterns);

    this.logFileNamePrefix = Optional.fromNullable(builder.logFileNamePrefix);

    this.linesWrittenBeforeFlush = builder.linesWrittenBeforeFlush;
    this.numCopyThreads = builder.numCopyThreads;

    this.executorService = Executors.newFixedThreadPool(numCopyThreads);
  }

  @Override
  protected void shutDown() throws Exception {
    try {
      //We need to copy the current log file as part of shutdown sequence.
      shouldCopyCurrentLogFile = true;
      runOneIteration();
      //Close the Filesystem objects, since these were created with auto close disabled.
      LOGGER.debug("Closing FileSystem objects...");
      this.destFs.close();
      this.srcFs.close();
    } finally {
      super.shutDown();
    }
  }

  @Override
  protected void runOneIteration() throws IOException {
    checkSrcLogFiles();
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedRateSchedule(0, this.sourceLogFileMonitorInterval, this.timeUnit);
  }

  private boolean shouldIncludeLogFile(FileStatus logFile) {
    Path logFilePath = logFile.getPath();

    //Skip copy of current log file if current log file copy is disabled
    if (currentLogFileName.equals(Files.getNameWithoutExtension(logFilePath.getName()))) {
      return shouldCopyCurrentLogFile;
    }

    //Skip copy of log file if it has already been copied previously.
    if (copiedFileNames.contains(logFilePath.getName())) {
      return false;
    }

    //Special case to accept all log file extensions.
    if (LogCopier.this.logFileExtensions.isEmpty()) {
      return true;
    }

    return LogCopier.this.logFileExtensions.contains(Files.getFileExtension(logFilePath.getName()));

  }

  /**
   * Prune the set of copied files by removing the set of files which have been already deleted from the source.
   * This keeps the copiedFileNames from growing unboundedly and is useful when log rotation is enabled on the
   * source dirs with maximum number of backups.
   * @param srcLogFileNames
   */
  @VisibleForTesting
  void pruneCopiedFileNames(Set<String> srcLogFileNames) {
    Iterator<String> copiedFilesIterator = copiedFileNames.iterator();

    while (copiedFilesIterator.hasNext()) {
      String fileName = copiedFilesIterator.next();
      if (!srcLogFileNames.contains(fileName)) {
        copiedFilesIterator.remove();
      }
    }
  }

  /**
   * Perform a check on new source log files and submit copy tasks for new log files.
   */
  @VisibleForTesting
  void checkSrcLogFiles() throws IOException {
    List<FileStatus> srcLogFiles = new ArrayList<>();
    Set<String> srcLogFileNames = new HashSet<>();
    Set <Path> newLogFiles = new HashSet<>();
    for (Path logDirPath: srcLogDirs) {
      srcLogFiles.addAll(FileListUtils.listFilesRecursively(srcFs, logDirPath));
      //Remove the already copied files from the list of files to copy
      for (FileStatus srcLogFile: srcLogFiles) {
        if (shouldIncludeLogFile(srcLogFile)) {
          newLogFiles.add(srcLogFile.getPath());
        }
        srcLogFileNames.add(srcLogFile.getPath().getName());
      }
    }

    if (newLogFiles.isEmpty()) {
      LOGGER.warn("No log file found under directories " + this.srcLogDirs);
      return;
    }

    List<Future> futures = new ArrayList<>();
    // Schedule a copy task for each new log file
    for (final Path srcLogFile : newLogFiles) {
      String destLogFileName = this.logFileNamePrefix.isPresent()
          ? this.logFileNamePrefix.get() + "." + srcLogFile.getName() : srcLogFile.getName();
      final Path destLogFile = new Path(this.destLogDir, destLogFileName);
      futures.add(this.executorService.submit(new LogCopyTask(srcLogFile, destLogFile)));
    }

    //Wait for copy tasks to finish
    for (Future future: futures) {
      try {
        future.get();
      } catch (InterruptedException e) {
        LOGGER.error("LogCopyTask was interrupted - {}", e);
      } catch (ExecutionException e) {
        LOGGER.error("Failed LogCopyTask - {}", e);
      }
    }

    pruneCopiedFileNames(srcLogFileNames);
  }

  /**
   * Get a new {@link LogCopier.Builder} instance for building a {@link LogCopier}.
   *
   * @return a new {@link LogCopier.Builder} instance
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * A builder class for {@link LogCopier}.
   */
  public static class Builder {

    private static final Splitter COMMA_SPLITTER = Splitter.on(',').omitEmptyStrings().trimResults();

    private FileSystem srcFs;
    private List<Path> srcLogDirs;
    private FileSystem destFs;
    private Path destLogDir;

    private long sourceLogFileMonitorInterval = DEFAULT_SOURCE_LOG_FILE_MONITOR_INTERVAL;

    private int numCopyThreads = DEFAULT_NUM_COPY_THREADS;

    private TimeUnit timeUnit = DEFAULT_TIME_UNIT;

    private Set<String> logFileExtensions;
    private String currentLogFileName;

    private List<Pattern> includingRegexPatterns;
    private List<Pattern> excludingRegexPatterns;

    private String logFileNamePrefix;

    private int linesWrittenBeforeFlush = DEFAULT_LINES_WRITTEN_BEFORE_FLUSH;

    /**
    * Set the interval between two checks for the source log file monitor.
    *
    * @param sourceLogFileMonitorInterval the interval between two checks for the source log file monitor
    * @return this {@link LogCopier.Builder} instance
    */
    public Builder useSourceLogFileMonitorInterval(long sourceLogFileMonitorInterval) {
      Preconditions.checkArgument(sourceLogFileMonitorInterval > 0,
          "Source log file monitor interval must be positive");
      this.sourceLogFileMonitorInterval = sourceLogFileMonitorInterval;
      return this;
    }

    /**
     * Set the {@link TimeUnit} used for the source log file monitor interval.
     *
     * @param timeUnit the {@link TimeUnit} used for the log file monitor interval
     * @return this {@link LogCopier.Builder} instance
     */
    public Builder useTimeUnit(TimeUnit timeUnit) {
      Preconditions.checkNotNull(timeUnit);
      this.timeUnit = timeUnit;
      return this;
    }

    /**
     * Set the set of acceptable log file extensions.
     *
     * @param logFileExtensions the set of acceptable log file extensions
     * @return this {@link LogCopier.Builder} instance
     */
    public Builder acceptsLogFileExtensions(Set<String> logFileExtensions) {
      Preconditions.checkNotNull(logFileExtensions);
      this.logFileExtensions = ImmutableSet.copyOf(logFileExtensions);
      return this;
    }

    /**
     * Set the regex patterns used to filter logs that should be copied.
     *
     * @param regexList a comma-separated list of regex patterns
     * @return this {@link LogCopier.Builder} instance
     */
    public Builder useIncludingRegexPatterns(String regexList) {
      Preconditions.checkNotNull(regexList);
      this.includingRegexPatterns = DatasetFilterUtils.getPatternsFromStrings(COMMA_SPLITTER.splitToList(regexList));
      return this;
    }

    /**
     * Set the regex patterns used to filter logs that should not be copied.
     *
     * @param regexList a comma-separated list of regex patterns
     * @return this {@link LogCopier.Builder} instance
     */
    public Builder useExcludingRegexPatterns(String regexList) {
      Preconditions.checkNotNull(regexList);
      this.excludingRegexPatterns = DatasetFilterUtils.getPatternsFromStrings(COMMA_SPLITTER.splitToList(regexList));
      return this;
    }

    /**
     * Set the source {@link FileSystem} for reading the source log file.
     *
     * @param srcFs the source {@link FileSystem} for reading the source log file
     * @return this {@link LogCopier.Builder} instance
     */
    public Builder useSrcFileSystem(FileSystem srcFs) {
      Preconditions.checkNotNull(srcFs);
      this.srcFs = srcFs;
      return this;
    }

    /**
     * Set the destination {@link FileSystem} for writing the destination log file.
     *
     * @param destFs the destination {@link FileSystem} for writing the destination log file
     * @return this {@link LogCopier.Builder} instance
     */
    public Builder useDestFileSystem(FileSystem destFs) {
      Preconditions.checkNotNull(destFs);
      this.destFs = destFs;
      return this;
    }

    /**
     * Set the path of the source log file directory to read from.
     *
     * @param srcLogDir the path of the source log file directory to read from
     * @return this {@link LogCopier.Builder} instance
     */
    public Builder readFrom(Path srcLogDir) {
      Preconditions.checkNotNull(srcLogDir);
      this.srcLogDirs = ImmutableList.of(srcLogDir);
      return this;
    }

    /**
     * Set the paths of the source log file directories to read from.
     *
     * @param srcLogDirs the paths of the source log file directories to read from
     * @return this {@link LogCopier.Builder} instance
     */
    public Builder readFrom(List<Path> srcLogDirs) {
      Preconditions.checkNotNull(srcLogDirs);
      this.srcLogDirs = srcLogDirs;
      return this;
    }

    /**
     * Set the path of the destination log file directory to write to.
     *
     * @param destLogDir the path of the destination log file directory to write to
     * @return this {@link LogCopier.Builder} instance
     */
    public Builder writeTo(Path destLogDir) {
      Preconditions.checkNotNull(destLogDir);
      this.destLogDir = destLogDir;
      return this;
    }

    /**
     * Set the log file name prefix at the destination.
     *
     * @param logFileNamePrefix the log file name prefix at the destination
     * @return this {@link LogCopier.Builder} instance
     */
    public Builder useLogFileNamePrefix(String logFileNamePrefix) {
      Preconditions.checkArgument(!Strings.isNullOrEmpty(logFileNamePrefix),
          "Invalid log file name prefix: " + logFileNamePrefix);
      this.logFileNamePrefix = logFileNamePrefix;
      return this;
    }

    /**
     * Set the number of lines written before they are flushed to disk.
     *
     * @param linesWrittenBeforeFlush the number of lines written before they are flushed to disk
     * @return this {@link LogCopier.Builder} instance
     */
    public Builder useLinesWrittenBeforeFlush(int linesWrittenBeforeFlush) {
      Preconditions.checkArgument(linesWrittenBeforeFlush > 0,
          "The value specifying the lines to write before flush must be positive");
      this.linesWrittenBeforeFlush = linesWrittenBeforeFlush;
      return this;
    }

    /**
     * Set the current log file name
     */
    public Builder useCurrentLogFileName(String currentLogFileName) {
      this.currentLogFileName = currentLogFileName;
      return this;
    }

    /**
     * Set the number of threads to use for copying container log files to dest FS.
     * @param numCopyThreads
     */
    public Builder useNumCopyThreads(int numCopyThreads) {
      this.numCopyThreads = numCopyThreads;
      return this;
    }

    /**
     * Build a new {@link LogCopier} instance.
     *
     * @return a new {@link LogCopier} instance
     */
    public LogCopier build() {
      return new LogCopier(this);
    }
  }

  private class LogCopyTask implements Callable<Void> {
    private final Path srcLogFile;
    private final Path destLogFile;

    public LogCopyTask(Path srcLogFile, Path destLogFile) {
      this.srcLogFile = srcLogFile;
      this.destLogFile = destLogFile;
    }

    @Override
    public Void call() {
      try {
        copyChangesOfLogFile(LogCopier.this.srcFs.makeQualified(this.srcLogFile),
            LogCopier.this.destFs.makeQualified(this.destLogFile));
      } catch (IOException ioe) {
        LOGGER.error(String.format("Failed while copying logs from %s to %s", this.srcLogFile, this.destLogFile), ioe);
      }
      return null;
    }

    /**
     * Copy changes for a single log file.
     */
    private void copyChangesOfLogFile(Path srcFile, Path destFile) throws IOException {
      LOGGER.info("Copying changes from {} to {}", srcFile.toString(), destFile.toString());
      if (!LogCopier.this.srcFs.exists(srcFile)) {
        LOGGER.warn("Source log file not found: " + srcFile);
        return;
      }

      // We need to use fsDataInputStream in the finally clause so it has to be defined outside try-catch-finally
      FSDataInputStream fsDataInputStream = null;

      try (Closer closer = Closer.create()) {
        fsDataInputStream = closer.register(LogCopier.this.srcFs.open(srcFile));
        BufferedReader srcLogFileReader = closer.register(
            new BufferedReader(new InputStreamReader(fsDataInputStream, ConfigurationKeys.DEFAULT_CHARSET_ENCODING)));

        FSDataOutputStream outputStream = LogCopier.this.destFs.create(destFile);
        BufferedWriter destLogFileWriter = closer.register(
            new BufferedWriter(new OutputStreamWriter(outputStream, ConfigurationKeys.DEFAULT_CHARSET_ENCODING)));

        String line;
        int linesProcessed = 0;
        while (!Thread.currentThread().isInterrupted() && (line = srcLogFileReader.readLine()) != null) {
          if (!shouldCopyLine(line)) {
            continue;
          }

          destLogFileWriter.write(line);
          destLogFileWriter.newLine();
          linesProcessed++;
          if (linesProcessed % LogCopier.this.linesWrittenBeforeFlush == 0) {
            destLogFileWriter.flush();
          }
        }
        //Add the copied file to the list of files already copied to the destination.
        LogCopier.this.copiedFileNames.add(srcFile.getName());
      }
    }

    /**
     * Check if a log line should be copied.
     *
     * <p>
     *   A line should be copied if and only if all of the following conditions satisfy:
     *
     *   <ul>
     *     <li>
     *       It doesn't match any of the excluding regex patterns. If there's no excluding regex patterns,
     *       this condition is considered satisfied.
     *     </li>
     *     <li>
     *       It matches at least one of the including regex patterns. If there's no including regex patterns,
     *       this condition is considered satisfied.
     *     </li>
     *   </ul>
     * </p>
     */
    private boolean shouldCopyLine(String line) {
      boolean including = !LogCopier.this.includingRegexPatterns.isPresent()
          || DatasetFilterUtils.stringInPatterns(line, LogCopier.this.includingRegexPatterns.get());
      boolean excluding = LogCopier.this.excludingRegexPatterns.isPresent()
          && DatasetFilterUtils.stringInPatterns(line, LogCopier.this.excludingRegexPatterns.get());

      return !excluding && including;
    }
  }
}
