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

package gobblin.runtime.job_monitor;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.mockito.Mockito;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.FetchedDataChunk;
import kafka.consumer.KafkaStream;
import kafka.consumer.PartitionTopicInfo;
import kafka.consumer.ZookeeperConsumerConnector;
import kafka.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.NoCompressionCodec$;
import kafka.serializer.DefaultDecoder;
import kafka.utils.VerifiableProperties;
import lombok.Getter;
import scala.collection.JavaConversions;


public class MockKafkaStream {

  private final List<BlockingQueue<FetchedDataChunk>> queues;
  @Getter
  private final List<KafkaStream<byte[], byte[]>> mockStreams;
  private final List<AtomicLong> offsets;
  private final AtomicLong nextStream;

  public MockKafkaStream(int numStreams) {

    this.queues = Lists.newArrayList();
    this.mockStreams = Lists.newArrayList();
    this.offsets = Lists.newArrayList();

    for (int i = 0; i < numStreams; i++) {
      BlockingQueue<FetchedDataChunk> queue = Queues.newLinkedBlockingQueue();
      this.queues.add(queue);
      this.mockStreams.add(createMockStream(queue));
      this.offsets.add(new AtomicLong(0));
    }

    this.nextStream = new AtomicLong(-1);
  }

  @SuppressWarnings("unchecked")
  private static KafkaStream<byte[], byte[]> createMockStream(BlockingQueue<FetchedDataChunk> queue) {

    KafkaStream<byte[], byte[]> stream = (KafkaStream<byte[], byte[]>) Mockito.mock(KafkaStream.class);
    ConsumerIterator<byte[], byte[]> it =
        new ConsumerIterator<>(queue, -1, new DefaultDecoder(new VerifiableProperties()), new DefaultDecoder(new VerifiableProperties()), "clientId");
    Mockito.when(stream.iterator()).thenReturn(it);

    return stream;
  }

  public void pushToStream(String message) {

    int streamNo = (int) this.nextStream.incrementAndGet() % this.queues.size();

    AtomicLong offset = this.offsets.get(streamNo);
    BlockingQueue<FetchedDataChunk> queue = this.queues.get(streamNo);

    AtomicLong thisOffset = new AtomicLong(offset.incrementAndGet());

    List<Message> seq = Lists.newArrayList();
    seq.add(new Message(message.getBytes(Charsets.UTF_8)));
    ByteBufferMessageSet messageSet = new ByteBufferMessageSet(NoCompressionCodec$.MODULE$, offset, JavaConversions.asScalaBuffer(seq));

    FetchedDataChunk chunk = new FetchedDataChunk(messageSet,
        new PartitionTopicInfo("topic", streamNo, queue, thisOffset, thisOffset, new AtomicInteger(1), "clientId"),
        thisOffset.get());

    queue.add(chunk);
  }

  public void shutdown() {
    for (BlockingQueue<FetchedDataChunk> queue : this.queues) {
      queue.add(ZookeeperConsumerConnector.shutdownCommand());
    }
  }

}
