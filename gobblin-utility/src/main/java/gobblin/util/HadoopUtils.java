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

package gobblin.util;

import gobblin.configuration.State;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Lists;


public class HadoopUtils {
  public static Configuration newConfiguration() {
    Configuration conf = new Configuration();

    // Explicitly check for S3 environment variables, so that Hadoop can access s3 and s3n URLs.
    // h/t https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/deploy/SparkHadoopUtil.scala
    String awsAccessKeyId = System.getenv("AWS_ACCESS_KEY_ID");
    String awsSecretAccessKey = System.getenv("AWS_SECRET_ACCESS_KEY");
    if (awsAccessKeyId != null && awsSecretAccessKey != null) {
      conf.set("fs.s3.awsAccessKeyId", awsAccessKeyId);
      conf.set("fs.s3.awsSecretAccessKey", awsSecretAccessKey);
      conf.set("fs.s3n.awsAccessKeyId", awsAccessKeyId);
      conf.set("fs.s3n.awsSecretAccessKey", awsSecretAccessKey);
    }

    return conf;
  }

  public static List<FileStatus> listStatusRecursive(FileSystem fileSystem, Path path) throws FileNotFoundException,
      IOException {
    List<FileStatus> results = Lists.newArrayList();
    walk(results, fileSystem, path);
    return results;
  }

  private static void walk(List<FileStatus> results, FileSystem fileSystem, Path path) throws FileNotFoundException,
      IOException {
    for (FileStatus status : fileSystem.listStatus(path)) {
      if (!status.isDir()) {
        results.add(status);
      } else {
        walk(results, fileSystem, status.getPath());
      }
    }
  }

  public static Configuration getConfFromState(State state) {
    Configuration conf = new Configuration();
    for (String propName : state.getPropertyNames()) {
      conf.set(propName, state.getProp(propName));
    }
    return conf;
  }
}
