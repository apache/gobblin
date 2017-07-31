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

package org.apache.gobblin.data.management.trash;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;


/**
 * Mock version of {@link org.apache.gobblin.data.management.trash.ProxiedTrash} for simulating deletions. Can also be used as
 * a mock for {@link org.apache.gobblin.data.management.trash.Trash}.
 */
public class MockTrash extends ProxiedTrash {

  private static final Logger LOG = LoggerFactory.getLogger(MockTrash.class);

  public MockTrash(FileSystem fs, Properties props, String user)
      throws IOException {
    super(fs, props, user);
  }

  @Override
  public boolean moveToTrash(Path path)
      throws IOException {
    LOG.info("Simulating move to trash: " + path);
    return true;
  }

  @Override
  public void createTrashSnapshot()
      throws IOException {
    throw new UnsupportedOperationException("Not supported for " + MockTrash.class);
  }

  @Override
  public void purgeTrashSnapshots()
      throws IOException {
    throw new UnsupportedOperationException("Not supported for " + MockTrash.class);
  }

  @Override
  protected Path createTrashLocation(FileSystem fs, Properties props, String user)
      throws IOException {
    return super.createTrashLocation(fs, props, user);
  }

  @Override
  protected List<String> getAllUsersWithTrash()
      throws IOException {
    return Lists.newArrayList();
  }

  @Override
  protected void ensureTrashLocationExists(FileSystem fs, Path trashLocation)
      throws IOException {
    // Do nothing
  }

  @Override
  protected Trash getUserTrash(String user)
      throws IOException {
    return this;
  }
}
