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

package org.apache.gobblin.azkaban;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.google.common.io.Files;

import azkaban.jobExecutor.AbstractJob;
import azkaban.utils.Props;
import azkaban.utils.PropsUtils;
import lombok.RequiredArgsConstructor;


/**
 * Runs Azkaban jobs locally.
 *
 * Usage:
 * Extend the class, in the constructor pass the list of relative paths to all common properties files, as well as list
 * of job files to run.
 *
 * Execution:
 * java -cp ... {@link AzkabanJobRunner} class-name root-directory
 *
 * Where class-name is the extension of {@link AzkabanJobRunner} that should be executed, and root-directory is the
 * root directory of the repository.
 *
 * @author Issac Buenrostro
 */
@RequiredArgsConstructor
public class AzkabanJobRunner {
  private File baseDirectory = new File(".");
  private final List<String> commonProps;
  private final List<String> jobProps;
  private final Map<String, String> overrides;

  static void doMain(Class<? extends AzkabanJobRunner> klazz, String[] args)
      throws Exception {
    AzkabanJobRunner runner = klazz.newInstance();
    if (args.length >= 1) {
      runner.setBaseDirectory(new File(args[0]));
    }
    runner.run();
  }

  public static String getTempDirectory() {
    File tmpDirectory = Files.createTempDir();
    tmpDirectory.deleteOnExit();
    return tmpDirectory.getAbsolutePath();
  }

  private void setBaseDirectory(File baseDirectory) {
    this.baseDirectory = baseDirectory;
  }

  public void run()
      throws IOException {

    Props commonProps = new Props();
    for (String commonPropsFile : this.commonProps) {
      commonProps = new Props(commonProps, new File(baseDirectory, commonPropsFile));
    }

    for (String jobFile : this.jobProps) {
      File file = new File(baseDirectory, jobFile);
      Props jobProps = new Props(new Props(commonProps, file), this.overrides);
      jobProps = PropsUtils.resolveProps(jobProps);
      try {
        AbstractJob job = constructAbstractJob(file.getName(), jobProps);
        job.run();
      } catch (Throwable t) {
        throw new RuntimeException(t);
      }
    }
  }

  private AbstractJob constructAbstractJob(String name, Props jobProps) {
    try {
      return (AbstractJob) jobProps.getClass("job.class").getConstructor(String.class, Props.class)
          .newInstance(name, jobProps);
    } catch (ReflectiveOperationException roe) {
      try {
        return (AbstractJob) jobProps.getClass("job.class").getConstructor(String.class, Properties.class)
            .newInstance(name, propsToProperties(jobProps));
      } catch (ReflectiveOperationException exc) {
        throw new RuntimeException(exc);
      }
    }
  }

  private Properties propsToProperties(Props props) {
    Properties properties = new Properties();
    for (String key : props.getKeySet()) {
      properties.put(key, props.getString(key));
    }
    return properties;
  }
}
