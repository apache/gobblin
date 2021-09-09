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
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Factory for creating {@link org.apache.gobblin.data.management.trash.Trash} instance. Will automatically use
 * {@link org.apache.gobblin.data.management.trash.TestTrash} if {@link #TRASH_TEST} is true,
 * {@link org.apache.gobblin.data.management.trash.MockTrash} if {@link #SIMULATE} is true,
 * and {@link org.apache.gobblin.data.management.trash.ImmediateDeletionTrash} if {@link #SKIP_TRASH} is true.
 * Otherwise, it will use {@link org.apache.gobblin.data.management.trash.ProxiedTrash} or {@link org.apache.gobblin.data.management.trash.Trash}.
 */
public class TrashFactory {

  private static final Logger LOG = LoggerFactory.getLogger(TrashFactory.class);

  public static final String TRASH_TEST = "gobblin.trash.test";
  public static final String SIMULATE = "gobblin.trash.simulate";
  public static final String SKIP_TRASH = "gobblin.trash.skip.trash";

  public static Trash createTrash(FileSystem fs) throws IOException {
    return createTrash(fs, new Properties());
  }

  public static Trash createTrash(FileSystem fs, Properties props) throws IOException {
    return createTrash(fs, props, UserGroupInformation.getCurrentUser().getShortUserName());
  }

  /**
   * Creates a {@link org.apache.gobblin.data.management.trash.Trash} instance.
   * @param fs {@link org.apache.hadoop.fs.FileSystem} where trash is located.
   * @param props {@link java.util.Properties} used to generate trash.
   * @param user $USER tokens in the trash path will be replaced by this string.
   * @return instance of {@link org.apache.gobblin.data.management.trash.Trash}.
   * @throws IOException
   */
  public static Trash createTrash(FileSystem fs, Properties props, String user)
      throws IOException {
    Trash trash = createTestMockOrImmediateDeletionTrash(fs, props, user);
    if (null != trash) {
      return trash;
    } else {
      return Trash.getTrash(fs, props, user);
    }
  }

  public static ProxiedTrash createProxiedTrash(FileSystem fs) throws IOException {
    return createProxiedTrash(fs, new Properties());
  }

  public static ProxiedTrash createProxiedTrash(FileSystem fs, Properties props) throws IOException {
    return createProxiedTrash(fs, props, UserGroupInformation.getCurrentUser().getShortUserName());
  }

  /**
   * Creates a {@link org.apache.gobblin.data.management.trash.ProxiedTrash} instance.
   * @param fs {@link org.apache.hadoop.fs.FileSystem} where trash is located.
   * @param props {@link java.util.Properties} used to generate trash.
   * @param user $USER tokens in the trash path will be replaced by this string.
   * @return instance of {@link org.apache.gobblin.data.management.trash.ProxiedTrash}.
   * @throws IOException
   */
  public static ProxiedTrash createProxiedTrash(FileSystem fs, Properties props, String user)
    throws IOException {
    ProxiedTrash trash = createTestMockOrImmediateDeletionTrash(fs, props, user);
    if (null != trash) {
      return trash;
    } else {
      return ProxiedTrash.getProxiedTrash(fs, props, user);
    }
  }

  /**
   * This creates {@link TestTrash}, {@link MockTrash} or {@link ImmediateDeletionTrash according to the properties set.
   * @param fs file system object
   * @param props properties
   * @param user user to create trash as
   * @return {@link TestTrash}, {@link MockTrash} or {@link ImmediateDeletionTrash or null if none of these trashes are
   * requested
   * @throws IOException
   */
  private static ProxiedTrash createTestMockOrImmediateDeletionTrash(FileSystem fs, Properties props, String user) throws IOException {
    if(props.containsKey(TRASH_TEST) && Boolean.parseBoolean(props.getProperty(TRASH_TEST))) {
      LOG.info("Creating a test trash. Nothing will actually be deleted.");
      return new TestTrash(fs, props, user);
    }
    if(props.containsKey(SIMULATE) && Boolean.parseBoolean(props.getProperty(SIMULATE))) {
      LOG.info("Creating a simulate trash. Nothing will actually be deleted.");
      return new MockTrash(fs, props, user);
    }
    if(props.containsKey(SKIP_TRASH) && Boolean.parseBoolean(props.getProperty(SKIP_TRASH))) {
      LOG.info("Creating an immediate deletion trash. Files will be deleted immediately instead of moved to trash.");
      return new ImmediateDeletionTrash(fs, props, user);
    }
    return null;
  }
}
