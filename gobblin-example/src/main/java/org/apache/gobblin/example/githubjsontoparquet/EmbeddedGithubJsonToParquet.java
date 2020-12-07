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
package org.apache.gobblin.example.githubjsontoparquet;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.cli.CommandLine;
import org.apache.gobblin.annotation.Alias;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.JobTemplate;
import org.apache.gobblin.runtime.api.SpecNotFoundException;
import org.apache.gobblin.runtime.cli.CliObjectSupport;
import org.apache.gobblin.runtime.cli.PublicMethodsGobblinCliFactory;
import org.apache.gobblin.runtime.embedded.EmbeddedGobblin;
import org.apache.gobblin.runtime.template.ResourceBasedJobTemplate;
import org.apache.gobblin.writer.WriterOutputFormat;
import org.codehaus.plexus.util.FileUtils;
import org.mortbay.log.Log;

import groovy.util.logging.Slf4j;


/**
 * Creates a CLI application for running app githubjsontoparquet.
 * Cli application takes two arguments:
 * 1st arg: Date time (yyyy-mm-dd-hh) of archive to pull, ex: 2015-01-01-25
 * 2nd arg: Work dir with filesystem URI (file:///home/someuser/somefolder)";
 * Run using:
 *  bin/gobblin run githubjsontoparquet 2017-12-14-15 file:///Users/someuser/somefolder
 * @author tilakpatidar
 */
public class EmbeddedGithubJsonToParquet extends EmbeddedGobblin {

  private static final String GITHUB_ARCHIVE_URL_TEMPLATE = "http://data.githubarchive.org/%s.json.gz";
  private static final String DOWNLOAD_DIR = "archives";
  private static final String ARCHIVE_SUFFIX = ".json.gz";
  private static final String WORK_DIR_KEY = "work.dir";

  @Slf4j
  @Alias(value = "githubjsontoparquet", description = "Extract Github data and write to parquet files")
  public static class CliFactory extends PublicMethodsGobblinCliFactory {

    public CliFactory() {
      super(EmbeddedGithubJsonToParquet.class);
    }

    @Override
    public EmbeddedGobblin constructEmbeddedGobblin(CommandLine cli)
        throws JobTemplate.TemplateException, IOException {
      String[] args = cli.getArgs();
      if (args.length < 1) {
        throw new RuntimeException("Expected 2 arguments. " + getUsageString());
      }
      try {
        if (args.length == 2) {
          return new EmbeddedGithubJsonToParquet(args[0], args[1]);
        }
      } catch (JobTemplate.TemplateException | IOException e) {
        e.printStackTrace();
      }
      return null;
    }

    @Override
    public String getUsageString() {
      return "<Date time (yyyy-mm-dd-hh) of archive to pull> <Work dir with file system URI>";
    }
  }

  @CliObjectSupport(argumentNames = {"archiveDateAndHour", "workDir"})
  public EmbeddedGithubJsonToParquet(String archiveDateAndHour, String workDir)
      throws JobTemplate.TemplateException, IOException {
    super("githubjsontoparquet");
    URL workDirUrl;
    try {
      workDirUrl = new URL(workDir);
    } catch (MalformedURLException e) {
      e.printStackTrace();
      throw new RuntimeException("Work directory URI with no protocol or malformed.");
    }

    // Set configuration
    String fsProtocol = workDirUrl.getProtocol() + ":///";
    this.setConfiguration(WORK_DIR_KEY, workDir);
    this.setConfiguration(ConfigurationKeys.FS_URI_KEY, fsProtocol);
    this.setConfiguration(ConfigurationKeys.STATE_STORE_ENABLED, "true");
    this.setConfiguration(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY, workDir + "/store");
    this.setConfiguration(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, fsProtocol);
    this.setConfiguration(ConfigurationKeys.DATA_PUBLISHER_FILE_SYSTEM_URI, fsProtocol);
    this.setConfiguration(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, workDir + "/event_data");
    this.setConfiguration(ConfigurationKeys.DATA_PUBLISHER_METADATA_OUTPUT_DIR, workDir + "/metadata");
    this.setConfiguration(ConfigurationKeys.WRITER_OUTPUT_FORMAT_KEY, WriterOutputFormat.PARQUET.toString());

    //Set template
    try {
      setTemplate(ResourceBasedJobTemplate.forResourcePath("githubjsontoparquet.template"));
    } catch (URISyntaxException | SpecNotFoundException e) {
      e.printStackTrace();
      throw new RuntimeException("Cannot set template");
    }

    // Download the archive
    String fileUrl = String.format(GITHUB_ARCHIVE_URL_TEMPLATE, archiveDateAndHour);
    Path downloadDirPath = createDownloadDir(workDirUrl.getPath(), fileUrl);
    Path downloadFile = getAbsoluteDownloadFilePath(downloadDirPath, archiveDateAndHour);
    downloadFile(fileUrl, downloadFile);
  }

  private Path getAbsoluteDownloadFilePath(Path downloadDirPath, String archiveDateAndHour) {
    String downloadFileName = archiveDateAndHour + ARCHIVE_SUFFIX;
    return Paths.get(downloadDirPath.toString(), downloadFileName);
  }

  private Path createDownloadDir(String workDir, String fileUrl) {
    Path downloadDirPath = Paths.get(workDir, DOWNLOAD_DIR);
    File downloadDirFile = downloadDirPath.toFile();
    try {
      Log.info(String.format("Creating download dir %s", downloadDirFile.toPath().toString()));
      FileUtils.forceMkdir(downloadDirFile);
    } catch (IOException e) {
      throw new RuntimeException(String
          .format("Unable to create download location for archive: %s at %s", fileUrl, downloadDirPath.toString()));
    }
    Log.info(String.format("Created download dir %s", downloadDirFile.toPath().toString()));
    return downloadDirPath;
  }

  private void downloadFile(String fileUrl, Path destination) {
    if (destination.toFile().exists()) {
      Log.info(String.format("Skipping download for %s at %s because destination already exists", fileUrl,
          destination.toString()));
      return;
    }

    try {
      URL archiveUrl = new URL(fileUrl);
      ReadableByteChannel rbc = Channels.newChannel(archiveUrl.openStream());
      FileOutputStream fos = new FileOutputStream(String.valueOf(destination));
      Log.info(String.format("Downloading %s at %s", fileUrl, destination.toString()));
      fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
      Log.info(String.format("Download complete for %s at %s", fileUrl, destination.toString()));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
