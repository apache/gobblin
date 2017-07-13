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

package gobblin.runtime;

import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Optional;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.Futures;

import gobblin.configuration.ConfigurationKeys;
import gobblin.converter.Converter;
import gobblin.fork.ForkOperator;
import gobblin.fork.Forker;
import gobblin.qualitychecker.row.RowLevelPolicyChecker;
import gobblin.records.RecordStreamWithMetadata;
import gobblin.runtime.fork.Fork;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.StreamingExtractor;
import gobblin.util.ExponentialBackoff;
import gobblin.writer.AcknowledgableWatermark;
import gobblin.writer.FineGrainedWatermarkTracker;
import gobblin.writer.WatermarkManager;
import gobblin.writer.WatermarkStorage;

import io.reactivex.flowables.ConnectableFlowable;
import io.reactivex.schedulers.Schedulers;
import lombok.AllArgsConstructor;


/**
 * A helper class to run {@link Task} in stream mode. Prevents {@link Task} from loading reactivex classes when not
 * needed.
 */
@AllArgsConstructor
public class StreamModelTaskRunner {

  private final Task task;

  private final TaskState taskState;
  private final Closer closer;
  private final TaskContext taskContext;
  private final Extractor extractor;
  private final Converter converter;
  private final RowLevelPolicyChecker rowChecker;
  private final TaskExecutor taskExecutor;
  private final ExecutionModel taskMode;
  private final AtomicBoolean shutdownRequested;
  private final Optional<FineGrainedWatermarkTracker> watermarkTracker;
  private final Optional<WatermarkManager> watermarkManager;
  private final Optional<WatermarkStorage> watermarkStorage;
  private final Map<Optional<Fork>, Optional<Future<?>>> forks;
  private final String watermarkingStrategy;

  protected void run() throws Exception {
    // Get the fork operator. By default IdentityForkOperator is used with a single branch.
    ForkOperator forkOperator = closer.register(this.taskContext.getForkOperator());

    RecordStreamWithMetadata<?, ?> stream = this.extractor.recordStream(this.shutdownRequested);
    ConnectableFlowable connectableStream = stream.getRecordStream().publish();
    stream = stream.withRecordStream(connectableStream);

    stream = stream.mapRecords(r -> {
      this.task.onRecordExtract();
      return r;
    });
    if (this.task.isStreamingTask()) {

      // Start watermark manager and tracker
      if (this.watermarkTracker.isPresent()) {
        this.watermarkTracker.get().start();
      }
      this.watermarkManager.get().start();

      ((StreamingExtractor) this.taskContext.getRawSourceExtractor()).start(this.watermarkStorage.get());

      stream = stream.mapRecords(r -> {
        AcknowledgableWatermark ackableWatermark = new AcknowledgableWatermark(r.getWatermark());
        if (watermarkTracker.isPresent()) {
          watermarkTracker.get().track(ackableWatermark);
        }
        return r.withAckableWatermark(ackableWatermark);
      });
    }
    if (this.converter instanceof MultiConverter) {
      // if multiconverter, unpack it
      for (Converter cverter : ((MultiConverter) this.converter).getConverters()) {
        stream = cverter.processStream(stream, this.taskState);
      }
    } else {
      stream = this.converter.processStream(stream, this.taskState);
    }
    stream = this.rowChecker.processStream(stream, this.taskState);

    Forker.ForkedStream<?, ?> forkedStreams = new Forker().forkStream(stream, forkOperator, this.taskState);

    boolean isForkAsync = !this.task.areSingleBranchTasksSynchronous(this.taskContext) || forkedStreams.getForkedStreams().size() > 1;
    int bufferSize =
        this.taskState.getPropAsInt(ConfigurationKeys.FORK_RECORD_QUEUE_CAPACITY_KEY, ConfigurationKeys.DEFAULT_FORK_RECORD_QUEUE_CAPACITY);

    for (int fidx = 0; fidx < forkedStreams.getForkedStreams().size(); fidx ++) {
      RecordStreamWithMetadata<?, ?> forkedStream = forkedStreams.getForkedStreams().get(fidx);
      if (forkedStream != null) {
        if (isForkAsync) {
          forkedStream = forkedStream.mapStream(f -> f.observeOn(Schedulers.from(this.taskExecutor.getForkExecutor()), false, bufferSize));
        }
        Fork fork = new Fork(this.taskContext, forkedStream.getSchema(), forkedStreams.getForkedStreams().size(), fidx, this.taskMode);
        fork.consumeRecordStream(forkedStream);
        this.forks.put(Optional.of(fork), Optional.of(Futures.immediateFuture(null)));
        this.task.configureStreamingFork(fork, this.watermarkingStrategy);
      }
    }

    connectableStream.connect();

    if (!ExponentialBackoff.awaitCondition().callable(() -> this.forks.keySet().stream().map(Optional::get).allMatch(Fork::isDone)).
        initialDelay(1000L).maxDelay(1000L).maxWait(TimeUnit.MINUTES.toMillis(60)).await()) {
      throw new TimeoutException("Forks did not finish withing specified timeout.");
    }
  }

}
