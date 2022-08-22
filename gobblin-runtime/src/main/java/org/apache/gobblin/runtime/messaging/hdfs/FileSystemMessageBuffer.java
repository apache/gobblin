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

import com.typesafe.config.Config;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import lombok.Getter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.gobblin.runtime.messaging.MessageBuffer;
import org.apache.gobblin.runtime.messaging.data.DynamicWorkUnitMessage;
import org.apache.gobblin.runtime.messaging.data.DynamicWorkUnitSerde;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


/**
 * Implements {@link FileSystem} based message buffer for sending and receiving {@link DynamicWorkUnitMessage}.
 */
public class FileSystemMessageBuffer implements MessageBuffer<DynamicWorkUnitMessage> {
  @Getter
  private String channelName;
  private boolean isPollingForMessages = false;
  private FileSystem fs;
  private Path dir;
  private Duration pollingRate;
  private final List<Consumer<List<DynamicWorkUnitMessage>>> subscribers = new ArrayList<>();

  public FileSystemMessageBuffer(FileSystem fs, String channelName, Duration pollingRate, Path parentWorkingDir) {
    this.fs = fs;
    this.channelName = channelName;
    this.pollingRate = pollingRate;
    this.dir = Path.mergePaths(parentWorkingDir, new Path(channelName));
  }

  @Override
  public void publish(DynamicWorkUnitMessage item) throws IOException {
    byte[] serializedMsg = DynamicWorkUnitSerde.serialize(item);
    persistMessage(serializedMsg);
  }

  @Override
  public void subscribe(Consumer<List<DynamicWorkUnitMessage>> subscriber) {
    subscribers.add(subscriber);

    if (!isPollingForMessages) {
      startPollingForNewMessages(dir);
      isPollingForMessages = true;
    }
  }

  private void startPollingForNewMessages(Path dirToPoll) {
    Factory.getExecutorInstance().scheduleAtFixedRate(() -> {
          List<DynamicWorkUnitMessage> newMessages = this.getNewMessages();
          if (!CollectionUtils.isEmpty(newMessages)) {
            subscribers.forEach(s -> s.accept(newMessages));
          }
        },
        0, pollingRate.toMillis(), TimeUnit.MILLISECONDS);
  }

  private List<DynamicWorkUnitMessage> getNewMessages() {
    // STUB: TODO GOBBLIN-1685 Read all files in dir, deserialize, then clean up byte files in dir
    return null;
  }

  private void persistMessage(byte[] serializeMsg) throws IOException {
    // STUB: TODO GOBBLIN-1685 write serialized message to FileSystem
  }

  public static class Factory implements MessageBuffer.Factory<DynamicWorkUnitMessage> {
    private static volatile ScheduledExecutorService executorSingleton;
    private Config cfg;
    public Factory(Config cfg) {
      // STUB: TODO GOBBLIN-1685 Add any necessary setup
      this.cfg = cfg;
    }

    @Override
    public MessageBuffer<DynamicWorkUnitMessage> getBuffer(String channelName) {
      // STUB: TODO GOBBLIN-1685 Construct buffer object based on Typesafe Config
      return null;
    }

    private FileSystem getFileSystem(Config cfg) {
      // STUB: TODO GOBBLIN-1685 Get filesystem based on current state
      return null;
    }

    private synchronized static ScheduledExecutorService getExecutorInstance() {
      if (executorSingleton == null) {
        executorSingleton = Executors.newScheduledThreadPool(1);
      }

      return executorSingleton;
    }
  }
}
