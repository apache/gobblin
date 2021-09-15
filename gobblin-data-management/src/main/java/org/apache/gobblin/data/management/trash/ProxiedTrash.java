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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.gobblin.util.reflection.GobblinConstructorUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;

import org.apache.gobblin.util.ProxiedFileSystemCache;

/**
 * An implementation of {@link org.apache.gobblin.data.management.trash.Trash} that allows deleting files as different users.
 * Uses {@link org.apache.gobblin.util.ProxiedFileSystemCache} to proxy as different users.
 */
public class ProxiedTrash extends Trash implements GobblinProxiedTrash {

  private final Cache<String, Trash> trashCache = CacheBuilder.newBuilder().maximumSize(100).build();
  private final Properties properties;

  public ProxiedTrash(FileSystem fs, Properties props, String user) throws IOException {
    super(fs, props, user);
    this.properties = props;
  }

  public static ProxiedTrash getProxiedTrash(FileSystem fs, Properties props, String user) throws IOException {
    if (props.containsKey(TRASH_CLASS_KEY)) {
      return GobblinConstructorUtils.invokeConstructor(ProxiedTrash.class, props.getProperty(TRASH_CLASS_KEY), fs, props, user);
    } else {
      return new ProxiedTrash(fs, props, user);
    }
  }

  /**
   * Move the path to trash as specified user.
   * @param path {@link org.apache.hadoop.fs.Path} to move.
   * @param user User to move the path as.
   * @return true if the move succeeded.
   * @throws IOException
   */
  @Override
  public boolean moveToTrashAsUser(Path path, final String user) throws IOException {
    return getUserTrash(user).moveToTrash(path);
  }

  /**
   * Move the path to trash as the owner of the path.
   * @param path {@link org.apache.hadoop.fs.Path} to move.
   * @return true if the move succeeded.
   * @throws IOException
   */
  public boolean moveToTrashAsOwner(Path path) throws IOException {
    String owner = this.fs.getFileStatus(path).getOwner();
    return moveToTrashAsUser(path, owner);
  }

  /**
   * Create a trash snapshot as the specified user.
   * @param user user to proxy.
   * @throws IOException
   */
  public void createTrashSnapshotAsUser(String user) throws IOException {
    getUserTrash(user).createTrashSnapshot();
  }

  /**
   * Purge trash snapshots as the specified user.
   * @param user user to proxy.
   * @throws IOException
   */
  public void purgeTrashSnapshotsAsUser(String user) throws IOException {
    getUserTrash(user).purgeTrashSnapshots();
  }

  /**
   * Create trash snapshots for all users with trash directories. These users are determined by listing all directories in
   * the file system matching the trash pattern given by {@link #TRASH_LOCATION_KEY}.
   * @throws IOException
   */
  public void createTrashSnapshotsForAllUsers() throws IOException {
    for (String user : getAllUsersWithTrash()) {
      createTrashSnapshotAsUser(user);
    }
  }

  /**
   * Purge trash snapshots for all users with trash directories. These users are determined by listing all directories in
   * the file system matching the trash pattern given by {@link #TRASH_LOCATION_KEY}.
   * @throws IOException
   */
  public void purgeTrashSnapshotsForAllUsers() throws IOException {
    for (String user : getAllUsersWithTrash()) {
      purgeTrashSnapshotsAsUser(user);
    }
  }

  /**
   * Find all users with trash directories by listing all directories in
   * the file system matching the trash pattern given by {@link #TRASH_LOCATION_KEY}.
   * @return List of users with trash directory.
   * @throws IOException
   */
  protected List<String> getAllUsersWithTrash() throws IOException {
    Path trashLocationGlob = new Path(this.properties.getProperty(TRASH_LOCATION_KEY).replaceAll("\\$USER", "*"));
    Pattern userPattern =
        Pattern.compile(this.properties.getProperty(TRASH_LOCATION_KEY).replaceAll("\\$USER", "([^/])"));

    List<String> users = Lists.newArrayList();
    for (FileStatus fileStatus : this.fs.globStatus(trashLocationGlob)) {
      Matcher matcher = userPattern.matcher(fileStatus.getPath().toString());
      if (matcher.find()) {
        users.add(matcher.group(1));
      }
    }

    return users;
  }

  /**
   * Get {@link org.apache.gobblin.data.management.trash.Trash} instance for the specified user.
   * @param user user for whom {@link org.apache.gobblin.data.management.trash.Trash} should be generated.
   * @return {@link org.apache.gobblin.data.management.trash.Trash} as generated by proxied user.
   * @throws IOException
   */
  protected Trash getUserTrash(final String user) throws IOException {
    if (UserGroupInformation.getCurrentUser().getShortUserName().equals(user)) {
      return this;
    }
    try {
      return this.trashCache.get(user, new Callable<Trash>() {
        @Override
        public Trash call() throws Exception {
          return createNewTrashForUser(ProxiedTrash.this.fs, ProxiedTrash.this.properties, user);
        }
      });
    } catch (ExecutionException ee) {
      throw new IOException("Failed to get trash for user " + user);
    }
  }

  protected Trash createNewTrashForUser(FileSystem fs, Properties properties, String user) throws IOException {
    return new Trash(
        ProxiedFileSystemCache.fromProperties().referenceFS(fs).properties(properties).userNameToProxyAs(user).build(),
        properties, user);
  }
}
