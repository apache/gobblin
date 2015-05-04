/* (c) 2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.source.extractor.extract.kafka;

import java.nio.ByteBuffer;

import kafka.message.MessageAndOffset;


/**
 * An implementation of KafkaEvent based on the old Kafka API.
 *
 * This class wraps a MessageAndOffset object, which is a class
 * in the old Kafka API.
 *
 * @author ziliu
 */
public class KafkaOldEvent implements KafkaEvent<ByteBuffer, ByteBuffer> {

  private final MessageAndOffset messageAndOffset;

  public KafkaOldEvent(MessageAndOffset messageAndOffset) {
    this.messageAndOffset = messageAndOffset;
  }

  /**
   * Retrieve the key of the event.
   */
  @Override
  public ByteBuffer key() {
    return messageAndOffset.message().key();
  }

  /**
   * Retrieve the value of the event.
   */
  @Override
  public ByteBuffer value() {
    return messageAndOffset.message().payload();
  }

  /**
   * Retrieve the offset of the event.
   */
  @Override
  public long offset() {
    return messageAndOffset.offset();
  }

  /**
   * Retrieve the next offset of the event.
   */
  @Override
  public long nextOffset() {
    return messageAndOffset.nextOffset();
  }

}
