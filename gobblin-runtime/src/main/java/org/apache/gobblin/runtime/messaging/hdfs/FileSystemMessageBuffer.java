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
package org.apache.gobblin.runtime.messaging.hdfs;

import com.google.common.io.ByteStreams;
import com.typesafe.config.Config;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.gobblin.runtime.messaging.MessageBuffer;
import org.apache.gobblin.runtime.messaging.data.DynamicWorkUnitMessage;
import org.apache.gobblin.runtime.messaging.data.DynamicWorkUnitSerde;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.PathUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import static org.apache.gobblin.runtime.messaging.DynamicWorkUnitConfigKeys.*;


/**
 * Implements {@link FileSystem} based message buffer for sending and receiving {@link DynamicWorkUnitMessage}.
 */
@Slf4j
public class FileSystemMessageBuffer implements MessageBuffer<DynamicWorkUnitMessage> {

  // Dynamic Workunit message specific file suffix
  private static final String messageFileSuffix = ".wumessage";

  private boolean isPollingTaskScheduled = false;

  @Getter
  private final String channelName;
  private final FileSystem fs;
  @Getter
  private final Path path;
  private final long pollingRateMillis;
  private final List<Consumer<List<DynamicWorkUnitMessage>>> subscribers = new ArrayList<>();

  private FileSystemMessageBuffer(FileSystem fs, String channelName, long pollingRateMillis, Path parentWorkingDir) {
    this.fs = fs;
    this.channelName = channelName;
    this.pollingRateMillis = pollingRateMillis;
    this.path = parentWorkingDir;
  }

  @Override
  public void publish(DynamicWorkUnitMessage item) throws IOException {
    byte[] serializedMsg = DynamicWorkUnitSerde.serialize(item);
    try (FSDataOutputStream os = fs.create(this.getFilePathForMessage(item), true)) {
      os.write(serializedMsg);
      log.info("Persisted message into HDFS path {} for workunit {}.", path.toString(), item.getWorkUnitId());
    } catch (IOException ioException) {
      log.error("Failed to persist message into HDFS path {} for workunit {}.",
          path.toString(), item.getWorkUnitId(), ioException);
      throw ioException;
    }
  }

  @Override
  public void subscribe(Consumer<List<DynamicWorkUnitMessage>> subscriber) {
    subscribers.add(subscriber);
    startPollingIfNotAlreadyPolling();
  }

  /**
   * Only need one scheduled executor task / thread for polling
   */
  private void startPollingIfNotAlreadyPolling() {
    if (!isPollingTaskScheduled) {
      scheduleTaskForPollingNewMessages();
      isPollingTaskScheduled = true;
    }
  }

  private void scheduleTaskForPollingNewMessages() {
    Factory.getExecutorInstance().scheduleAtFixedRate(() -> {
          List<DynamicWorkUnitMessage> newMessages = this.getNewMessages();
          if (!CollectionUtils.isEmpty(newMessages)) {
            subscribers.forEach(s -> s.accept(newMessages));
          }
        },
        0, pollingRateMillis, TimeUnit.MILLISECONDS);
  }

  private List<DynamicWorkUnitMessage> getNewMessages() {
    List<DynamicWorkUnitMessage> messageList = new LinkedList<>();
    try {
      FileStatus[] fileStatus = fs.listStatus(path);
      for(FileStatus status : fileStatus){
        Path singlePath = status.getPath();
        if (fs.isFile(singlePath))
        {
          FSDataInputStream fis = fs.open(singlePath);
          DynamicWorkUnitMessage message = DynamicWorkUnitSerde.deserialize(ByteStreams.toByteArray(fis));
          messageList.add(message);
          fs.delete(singlePath, true);
          log.debug("Fetched message from HDFS path {}.", singlePath.toString());
        }
      }
    } catch (IOException ioException) {
      log.error("Failed to get message from HDFS path {}.", path.toString(), ioException);
    }

    return messageList;
  }

  private Path getFilePathForMessage(DynamicWorkUnitMessage message) {
    String messageFileName = message.getWorkUnitId() + "_" + System.currentTimeMillis();
    Path messagePath = PathUtils.mergePaths(path, new Path(messageFileName));
    return PathUtils.addExtension(messagePath, messageFileSuffix);
  }

  public static class Factory implements MessageBuffer.Factory<DynamicWorkUnitMessage> {
    private static volatile ScheduledExecutorService executorSingleton;
    private final Config cfg;
    private Path basePath;
    private FileSystem fileSystem;
    private long pollingRate;
    public Factory(Config cfg) {
      this.cfg = cfg;
      this.setup();
    }

    @Override
    public MessageBuffer<DynamicWorkUnitMessage> getBuffer(String HDFSFolder) {
      Path workunitPath = new Path(basePath, HDFSFolder);
      log.info("HDFS directory for dynamic workunit is: " + workunitPath);

      return new FileSystemMessageBuffer(fileSystem, HDFSFolder, pollingRate, workunitPath);
    }

    private void setup() {
      String fsPathString = this.cfg.getString(DYNAMIC_WORKUNIT_HDFS_PATH);
      this.basePath = new Path(fsPathString);
      try {
        this.fileSystem = this.basePath.getFileSystem(new Configuration());
      } catch (IOException e) {
        throw new RuntimeException("Unable to detect workunit directory file system:", e);
      }
      pollingRate = ConfigUtils.getLong(this.cfg, DYNAMIC_WORKUNIT_HDFS_POLLING_RATE_MILLIS,
          DYNAMIC_WORKUNIT_HDFS_DEFAULT_POLLING_RATE);
    }

    private synchronized static ScheduledExecutorService getExecutorInstance() {
      if (executorSingleton == null) {
        executorSingleton = Executors.newScheduledThreadPool(1);
      }
      return executorSingleton;
    }
  }
}