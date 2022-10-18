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

package org.apache.gobblin.writer;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.impl.MemoryManagerImpl;

import lombok.extern.slf4j.Slf4j;


/**
 * A thin layer extending {@link MemoryManagerImpl} for logging and instrumentation purpose.
 */
@Slf4j
public class GobblinOrcMemoryManager extends MemoryManagerImpl {
  public GobblinOrcMemoryManager(Configuration conf) {
    super(conf);
    log.info("The pool reserved for memory manager is :{}", getTotalMemoryPool());
  }

  @Override
  public synchronized void addWriter(Path path, long requestedAllocation, Callback callback)
      throws IOException {
    super.addWriter(path, requestedAllocation, callback);
    log.info("Adding writer for Path {}, Current allocation: {}", path.toString(), getAllocationScale());
  }

  @Override
  public synchronized void removeWriter(Path path)
      throws IOException {
    super.removeWriter(path);
    log.info("Closing writer for Path {}, Current allocation: {}", path.toString(), getAllocationScale());
  }
}
