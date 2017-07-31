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
package org.apache.gobblin.compliance.purger;

import java.io.IOException;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.writer.DataWriter;


/**
 * This class is responsible for executing all purge queries and altering the partition location if the original
 * partition is not modified during the current execution.
 *
 * @author adsharma
 */
@Slf4j
@AllArgsConstructor
public class HivePurgerWriter implements DataWriter<PurgeableHivePartitionDataset> {

  @Override
  public void write(PurgeableHivePartitionDataset dataset)
      throws IOException {
    dataset.purge();
  }

  @Override
  public long recordsWritten() {
    return 1;
  }

  /**
   * Following methods are not implemented by this class
   * @throws IOException
   */
  @Override
  public void commit()
      throws IOException {

  }

  @Override
  public void close()
      throws IOException {
  }

  @Override
  public void cleanup()
      throws IOException {
  }

  @Override
  public long bytesWritten()
      throws IOException {
    return 0;
  }
}
