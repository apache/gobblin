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

package org.apache.gobblin.data.management.retention;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import azkaban.jobExecutor.AbstractJob;
import azkaban.utils.Props;


/**
 * Job to run {@link org.apache.gobblin.data.management.retention.DatasetCleanerNew} job in Azkaban or Hadoop.
 */
public class DatasetCleanerJob extends AbstractJob implements Tool {

  private Configuration conf;
  private DatasetCleaner datasetCleaner;

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new DatasetCleanerJob(DatasetCleanerJob.class.getName()), args);
  }

  public DatasetCleanerJob(String id) throws Exception {
    super(id, Logger.getLogger(DatasetCleanerJob.class));
  }

  public DatasetCleanerJob(String id, Properties props) throws IOException {
    super(id, Logger.getLogger(DatasetCleanerJob.class));
    this.conf = new Configuration();
    this.datasetCleaner = new DatasetCleaner(FileSystem.get(getConf()), props);
  }

  public DatasetCleanerJob(String id, Props props) throws IOException {
    super(id, Logger.getLogger(DatasetCleanerJob.class));
    this.conf = new Configuration();
    this.datasetCleaner = new DatasetCleaner(FileSystem.get(getConf()), props.toProperties());
  }

  @Override
  public void run() throws Exception {
    if (this.datasetCleaner != null) {
      try {
        this.datasetCleaner.clean();
      } finally {
        this.datasetCleaner.close();
      }
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 1) {
      System.out.println("Must provide properties file as first argument.");
      return 1;
    }
    Props props = new Props(null, args[0]);
    new DatasetCleanerJob(DatasetCleanerJob.class.getName(), props).run();
    return 0;
  }

  @Override
  public void setConf(Configuration configuration) {
    this.conf = configuration;
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }
}
