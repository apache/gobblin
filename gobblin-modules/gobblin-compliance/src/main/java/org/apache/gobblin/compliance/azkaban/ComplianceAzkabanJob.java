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
package org.apache.gobblin.compliance.azkaban;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

import azkaban.jobExecutor.AbstractJob;
import azkaban.utils.Props;

import org.apache.gobblin.compliance.ComplianceJob;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;

import static org.apache.gobblin.compliance.ComplianceConfigurationKeys.GOBBLIN_COMPLIANCE_JOB_CLASS;


/**
 * Class to run compliance job on Azkaban.
 * A property gobblin.compliance.job.class needs to be passed and the appropriate compliance job will run.
 *
 * @author adsharma
 */
public class ComplianceAzkabanJob extends AbstractJob implements Tool {

  private Configuration conf;
  private ComplianceJob complianceJob;

  public static void main(String[] args)
      throws Exception {
    ToolRunner.run(new ComplianceAzkabanJob(ComplianceAzkabanJob.class.getName()), args);
  }

  public ComplianceAzkabanJob(String id)
      throws Exception {
    super(id, Logger.getLogger(ComplianceAzkabanJob.class));
  }

  public ComplianceAzkabanJob(String id, Props props)
      throws IOException {
    super(id, Logger.getLogger(ComplianceAzkabanJob.class));
    this.conf = new Configuration();
    // new prop
    Properties properties = props.toProperties();
    Preconditions.checkArgument(properties.containsKey(GOBBLIN_COMPLIANCE_JOB_CLASS),
        "Missing required property " + GOBBLIN_COMPLIANCE_JOB_CLASS);
    String complianceJobClass = properties.getProperty(GOBBLIN_COMPLIANCE_JOB_CLASS);
    this.complianceJob = GobblinConstructorUtils.invokeConstructor(ComplianceJob.class, complianceJobClass, properties);
  }

  @Override
  public void run()
      throws Exception {
    if (this.complianceJob != null) {
      this.complianceJob.run();
    }
  }

  @Override
  public int run(String[] args)
      throws Exception {
    if (args.length < 1) {
      System.out.println("Must provide properties file as first argument.");
      return 1;
    }
    Props props = new Props(null, args[0]);
    new ComplianceAzkabanJob(ComplianceAzkabanJob.class.getName(), props).run();
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
