/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.data.management.trash;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Lists;

import lombok.Data;
import lombok.Getter;


/**
 * Implementation of {@link ProxiedTrash} to use for testing. All operations in this implementation are noop, but user
 * can get all delete operations executed using {@link #getDeleteOperations}. This implementation does not use the
 * file system at all, so user can use a minimally mocked file system.
 */
public class TestTrash extends MockTrash {

  private static final String DELAY_MILLIS_KEY = "gobblin.trash.test.delays.millis";

  /**
   * Creates {@link java.util.Properties} that will generate a {@link gobblin.data.management.trash.TestTrash} when
   * using {@link gobblin.data.management.trash.TrashFactory}.
   */
  public static Properties propertiesForTestTrash() {
    Properties properties = new Properties();
    properties.setProperty(TrashFactory.TRASH_TEST, Boolean.toString(true));
    return properties;
  }

  /**
   * Mutates properties so that creating a TestTrash with this properties object will simulate delay in the
   * filesystem.
   * @param properties {@link Properties} used for building a test trash.
   * @param delay All calls to {@link TestTrash} involving file system will simulate a delay of this many milliseconds.
   */
  public static void simulateDelay(Properties properties, long delay) {
    properties.setProperty(DELAY_MILLIS_KEY, Long.toString(delay));
  }

  /**
   * Abstraction for a delete operation. Stores deleted {@link org.apache.hadoop.fs.Path} and user proxied for the
   * deletion. When calling {@link #moveToTrash}, {@link #user} is set to null.
   */
  @Data
  public static class DeleteOperation {
    private final Path path;
    private final String user;
  }

  @Getter
  private final List<DeleteOperation> deleteOperations;
  private final String user;
  private final long delay;

  @Getter
  private final boolean simulate;
  @Getter
  private final boolean skipTrash;

  public TestTrash(FileSystem fs, Properties props, String user)
      throws IOException {
    super(fs, propertiesForConstruction(props), user);
    this.user = user;
    this.deleteOperations = Lists.newArrayList();
    this.simulate = props.containsKey(TrashFactory.SIMULATE) &&
        Boolean.parseBoolean(props.getProperty(TrashFactory.SIMULATE));
    this.skipTrash = props.containsKey(TrashFactory.SKIP_TRASH) &&
        Boolean.parseBoolean(props.getProperty(TrashFactory.SKIP_TRASH));
    if(props.containsKey(DELAY_MILLIS_KEY)) {
      this.delay = Long.parseLong(props.getProperty(DELAY_MILLIS_KEY));
    } else {
      this.delay = 0;
    }
  }

  @Override
  public boolean moveToTrash(Path path)
      throws IOException {
    waitForMillis(this.delay);
    addDeleteOperation(new DeleteOperation(path, null));
    return true;
  }

  @Override
  public boolean moveToTrashAsUser(Path path, String user)
      throws IOException {
    waitForMillis(this.delay);
    addDeleteOperation(new DeleteOperation(path, user));
    return true;
  }

  @Override
  public boolean moveToTrashAsOwner(Path path)
      throws IOException {
    waitForMillis(this.delay);
    return moveToTrashAsUser(path, this.user);
  }

  private synchronized void addDeleteOperation(DeleteOperation dop) {
    this.deleteOperations.add(dop);
  }

  private void waitForMillis(long millis) {
    if(millis <= 0) {
      return;
    }
    try {
      Thread.sleep(millis);
    } catch (InterruptedException ie) {
      // return
    }
  }

  private static Properties propertiesForConstruction(Properties properties) {
    Properties newProperties = new Properties();
    newProperties.putAll(properties);
    newProperties.setProperty(Trash.SNAPSHOT_CLEANUP_POLICY_CLASS_KEY,
        NoopSnapshotCleanupPolicy.class.getCanonicalName());
    newProperties.setProperty(Trash.TRASH_LOCATION_KEY, "/test/path");
    return newProperties;
  }


}
