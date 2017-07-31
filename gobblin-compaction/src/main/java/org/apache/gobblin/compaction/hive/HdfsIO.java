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

package org.apache.gobblin.compaction.hive;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;


/**
 * Management for HDFS reads and writes.
 */
public abstract class HdfsIO {
  private static final Logger LOG = LoggerFactory.getLogger(HdfsIO.class);

  private static final String HDFS_URI = "hdfs.uri";
  private static final String HDFS_URI_DEFAULT = "hdfs://localhost:9000";
  private static final String HADOOP_CONFIGFILE_ = "hadoop.configfile.";
  private static final String HDFS_URI_HADOOP = "fs.defaultFS";

  @Deprecated // Gobblin only supports Hadoop 2.x.x
  private static final String HADOOP_VERSION = "hadoop.version";

  protected final String filePathInHdfs;
  protected final FileSystem fileSystem;

  public HdfsIO(String filePathInHdfs) throws IOException {
    this.filePathInHdfs = filePathInHdfs;
    this.fileSystem = getFileSystem();
  }

  protected static FileSystem getFileSystem() throws IOException {
    Configuration conf = getConfiguration();
    return FileSystem.get(conf);
  }

  protected static Configuration getConfiguration() {
    Configuration conf = new Configuration();
    addResourceToConf(conf);
    return conf;
  }

  private static void addResourceToConf(Configuration conf) {
    addHadoopConfigPropertiesToConf(conf);
    if (CompactionRunner.properties.containsKey(HDFS_URI)) {
      conf.set(HDFS_URI_HADOOP, CompactionRunner.properties.getProperty(HDFS_URI));
    }
    if (Strings.isNullOrEmpty(conf.get(HDFS_URI_HADOOP))) {
      conf.set(HDFS_URI_HADOOP, HDFS_URI_DEFAULT);
    }
    CompactionRunner.properties.setProperty(HDFS_URI, conf.get(HDFS_URI_HADOOP));
  }

  private static void addHadoopConfigPropertiesToConf(Configuration conf) {
    Set<String> propertyNames = CompactionRunner.properties.stringPropertyNames();
    for (String propertyName : propertyNames) {
      if (propertyName.startsWith(HADOOP_CONFIGFILE_)) {
        String hadoopConfigFile = CompactionRunner.properties.getProperty(propertyName);
        conf.addResource(new Path(hadoopConfigFile));
        LOG.info("Added Hadoop Config File: " + hadoopConfigFile);
      }
    }
  }

  public static String getHdfsUri() {
    return CompactionRunner.properties.getProperty(HDFS_URI, HDFS_URI_DEFAULT);
  }
}
