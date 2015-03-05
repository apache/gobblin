/* (c) 2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.compaction;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;


/**
 * Management for HDFS reads and writes.
 *
 * @author ziliu
 */
public abstract class HdfsIO {
  private static final Logger LOG = LoggerFactory.getLogger(HdfsIO.class);

  private static final String HDFS_URI = "hdfs.uri";
  private static final String HDFS_URI_DEFAULT = "hdfs://localhost:9000";
  private static final String HADOOP_CONFIGFILE_ = "hadoop.configfile.";
  private static final String HDFS_URI_HADOOP1 = "fs.default.name";
  private static final String HDFS_URI_HADOOP2 = "fs.defaultFS";
  private static final String HADOOP_VERSION = "hadoop.version";
  private static final String HADOOP_VERSION_DEFAULT = "2";
  private static final Set<Integer> VALID_HADOOP_VERSIONS = ImmutableSet.<Integer>builder().add(1).add(2).build();

  protected final String filePathInHdfs;
  protected final FileSystem fileSystem;

  public HdfsIO(String filePathInHdfs) throws IOException {
    this.filePathInHdfs = filePathInHdfs;
    this.fileSystem = getFileSystem();
  }

  private static String getHdfsUriHadoopPropertyName() {
    int hadoopVersion = getHadoopVersion();
    if (hadoopVersion == 1) {
      return HDFS_URI_HADOOP1;
    } else if (hadoopVersion == 2) {
      return HDFS_URI_HADOOP2;
    } else {
      String message = hadoopVersion + " is not a valid Hadoop version.";
      LOG.error(message);
      throw new RuntimeException(message);
    }
  }

  private static int getHadoopVersion() {
    int hadoopVersion =
        Integer.parseInt(CompactionRunner.properties.getProperty(HADOOP_VERSION, HADOOP_VERSION_DEFAULT));
    if (!VALID_HADOOP_VERSIONS.contains(hadoopVersion)) {
      String message = hadoopVersion + " is not a valid Hadoop version.";
      LOG.error(message);
      throw new RuntimeException(message);
    }
    return hadoopVersion;
  }

  protected static FileSystem getFileSystem() throws IOException {
    Configuration conf = getConfiguration();
    return FileSystem.get(conf);
  }

  protected static Configuration getConfiguration() throws FileNotFoundException {
    Configuration conf = new Configuration();
    addResourceToConf(conf);
    return conf;
  }

  private static void addResourceToConf(Configuration conf) {
    conf.setStrings(getHdfsUriHadoopPropertyName(), HDFS_URI_DEFAULT);
    addHadoopConfigPropertiesToConf(conf);
    if (CompactionRunner.properties.containsKey(HDFS_URI)) {
      conf.setStrings(getHdfsUriHadoopPropertyName(), CompactionRunner.properties.getProperty(HDFS_URI));
    }
  }

  private static void addHadoopConfigPropertiesToConf(Configuration conf) {
    Set<String> propertyNames = CompactionRunner.properties.stringPropertyNames();
    for (String propertyName : propertyNames) {
      if (propertyName.startsWith(HADOOP_CONFIGFILE_)) {
        String hadoopConfigFile = CompactionRunner.properties.getProperty(propertyName);
        conf.addResource(hadoopConfigFile);
        LOG.info("Added Hadoop Config File: " + hadoopConfigFile);
      }
    }
  }

  public static String getHdfsUri() {
    return CompactionRunner.properties.getProperty(HDFS_URI, HDFS_URI_DEFAULT);
  }
}
