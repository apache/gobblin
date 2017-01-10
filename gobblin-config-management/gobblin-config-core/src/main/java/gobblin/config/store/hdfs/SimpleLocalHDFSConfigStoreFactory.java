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

package gobblin.config.store.hdfs;

import org.apache.hadoop.fs.Path;


/**
 * Extension of {@link SimpleHDFSConfigStoreFactory} that creates a {@link SimpleHDFSConfigStore} which works for the
 * local file system.
 */
public class SimpleLocalHDFSConfigStoreFactory extends SimpleHDFSConfigStoreFactory {

  private static final String LOCAL_HDFS_SCHEME_NAME = "file";

  @Override
  protected String getPhysicalScheme() {
    return LOCAL_HDFS_SCHEME_NAME;
  }

  @Override
  protected Path getDefaultRootDir() {
    return new Path(System.getProperty("user.dir"));
  }
}
