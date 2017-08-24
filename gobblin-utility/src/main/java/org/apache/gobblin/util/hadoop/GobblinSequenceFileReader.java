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

package org.apache.gobblin.util.hadoop;

import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;


/**
 * Override the {@link SequenceFile.Reader} mainly to
 * override the {@link SequenceFile.Reader {@link #getValueClassName()}} so that
 * we can handle the package name issue properly.
 */
@Slf4j
public class GobblinSequenceFileReader extends SequenceFile.Reader {
  public GobblinSequenceFileReader(FileSystem fs, Path file,
      Configuration conf) throws IOException {
    super(fs, file, conf);
  }

  /** Returns the name of the value class. */
  public String getValueClassName() {
    if (super.getValueClassName().startsWith("gobblin.")) {
      log.info("[We have]   " + super.getValueClassName());
      return "org.apache." + super.getValueClassName();
    }

    return super.getValueClassName();
  }
}
