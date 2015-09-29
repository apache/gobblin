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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import lombok.Data;
import lombok.Getter;


/**
 * Implementation of {@link ProxiedTrash} to use for testing. All operations in this implementation are noop, but user
 * can get all delete operations executed using {@link #getDeleteOperations}. This implementation does not use the
 * file system at all, so user can use a minimally mocked file system.
 *
 * <p>
 *   This class optionally support simulating file system delay with an internal clock. The clock does not advance
 *   by itself, allowing programmers fine testing over a file system with delay.
 * </p>
 */
public class TestTrash extends MockTrash {

  private static final Logger LOGGER = LoggerFactory.getLogger(TestTrash.class);

  private static final String DELAY_TICKS_KEY = "gobblin.trash.test.delays.ticks";

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
   *
   * <p>
   *   When simulating delay, any operation related to the filesystem will initially block indefinitely. The test
   *   trash uses an internal clock that must be advanced by the user (it does not advance by itself).
   *   Operations are blocked for a specified number of ticks in the clock. To tick, the user must call the
   *   {@link #tick} method.
   * </p>
   *
   * <p>
   *   For example, if delay is 2:
   *   * User calls testTrash.moveToTrash(new Path("/"))  -> call blocks indefinitely, nothing added to delete operations
   *                                                         list.
   *   * User calls testTrash.tick()   -> call still blocked.
   *   * User calls testTrash.tick()   -> moveToTrash call returns, operation added to delete operations list.
   * </p>
   *
   * @param properties {@link Properties} used for building a test trash.
   * @param delay All calls to {@link TestTrash} involving file system will simulate a delay of this many ticks.
   */
  public static void simulateDelay(Properties properties, long delay) {
    properties.setProperty(DELAY_TICKS_KEY, Long.toString(delay));
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
  private long clockState;
  private final Lock lock;
  private final Condition clockStateUpdated;
  private final Condition signalReceived;

  private final AtomicLong callsReceivedSignal;
  private final AtomicLong operationsWaiting;
  private final AtomicLong operationsReceived;

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

    this.operationsReceived = new AtomicLong();

    this.lock = new ReentrantLock();
    this.clockStateUpdated = lock.newCondition();
    this.signalReceived = lock.newCondition();
    this.clockState = 0;
    this.operationsWaiting = new AtomicLong();
    this.callsReceivedSignal = new AtomicLong();
    if(props.containsKey(DELAY_TICKS_KEY)) {
      this.delay = Long.parseLong(props.getProperty(DELAY_TICKS_KEY));
    } else {
      this.delay = 0;
    }
  }

  @Override
  public boolean moveToTrash(Path path)
      throws IOException {
    this.operationsReceived.incrementAndGet();
    addDeleteOperation(new DeleteOperation(path, null));
    return true;
  }

  @Override
  public boolean moveToTrashAsUser(Path path, String user)
      throws IOException {
    this.operationsReceived.incrementAndGet();
    addDeleteOperation(new DeleteOperation(path, user));
    return true;
  }

  @Override
  public boolean moveToTrashAsOwner(Path path)
      throws IOException {
    return moveToTrashAsUser(path, this.user);
  }

  public long getOperationsReceived() {
    return this.operationsReceived.get();
  }

  public long getOperationsWaiting() {
    return this.operationsWaiting.get();
  }

  /**
   * Advance the internal clock by one tick. The call will block until all appropriate threads finish adding their
   * {@link DeleteOperation}s to the list.
   */
  public void tick() {

    this.lock.lock();

    // Advance clock
    this.clockState++;

    // Acquire lock, register how many threads are waiting for signal
    long callsAwaitingSignalOld = this.operationsWaiting.get();
    this.callsReceivedSignal.set(0);
    this.operationsWaiting.set(0);

    // Send signal
    this.clockStateUpdated.signalAll();

    try {
      while(this.callsReceivedSignal.get() < callsAwaitingSignalOld) {
        // this will release the lock, and it will periodically compare the number of threads that were awaiting
        // signal against the number of threads that have already received the signal. Therefore, this statement
        // will block until all threads have acked signal.
        this.signalReceived.await();
      }
    } catch (InterruptedException ie) {
      // Interrupted
    } finally {
      this.lock.unlock();
    }

  }

  private void addDeleteOperation(DeleteOperation dop) {

    // Acquire lock
    this.lock.lock();

    // Figure out when the operation can return
    long executeAt = this.clockState + this.delay;
    boolean firstLoop = true;

    try {
      // If delay is 0, this continues immediately.
      while(this.clockState < executeAt) {
        // If this is not the first loop, it means we have received a signal from tick, but still not at
        // appropriate clock state. Ack the receive (this is done here because if it is ready to "delete", it should
        // only ack after actually adding the DeleteOperation to the list).
        if(!firstLoop) {
          this.callsReceivedSignal.incrementAndGet();
          this.signalReceived.signalAll();
        }
        firstLoop = false;
        // Add itself to the list of calls awaiting signal
        this.operationsWaiting.incrementAndGet();
        // Await for signal that the clock has been updated
        this.clockStateUpdated.await();
      }
      // Perform "delete" operation, i.e. add DeleteOperation to list
      this.deleteOperations.add(dop);

      // Ack receipt of signal
      this.callsReceivedSignal.incrementAndGet();
      this.signalReceived.signal();
    } catch (InterruptedException ie) {
      // Interrupted
    } finally {
      this.lock.unlock();
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
