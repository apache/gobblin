/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package gobblin.eventhub.writer;
import gobblin.annotation.Alpha;
import gobblin.writer.Batch;

import java.util.Base64;
import java.util.LinkedList;
import java.util.List;


/**
 * The Eventhub Batch which internally saves each record
 * For now we are using LinkedList as our internal memory storage
 */
@Alpha
public class EventhubBatch extends Batch<byte[]>{
  private RecordMemory memory;
  private final long creationTimestamp;
  private final long memSizeLimit;
  private final long ttlInMilliSeconds;
  public static final int OVERHEAD_SIZE_IN_BYTES = 15;

  public EventhubBatch (long memSizeLimit, long ttlInMilliSeconds) {
    this.creationTimestamp = System.currentTimeMillis();
    this.memory = new RecordMemory();
    this.memSizeLimit = memSizeLimit;
    this.ttlInMilliSeconds = ttlInMilliSeconds;
  }

  public boolean isTTLExpire() {
    return (System.currentTimeMillis() - creationTimestamp) >= ttlInMilliSeconds;
  }

  private long getInternalSize(byte[] record) {
    return Base64.getEncoder().encodeToString(record).length() + this.OVERHEAD_SIZE_IN_BYTES;
  }

  public  class RecordMemory {
    private List<byte[]> records;
    private long byteSize;

    public RecordMemory () {
      byteSize = 0;
      records = new LinkedList<>();
    }

    void append (byte[] record) {
      byteSize += EventhubBatch.this.getInternalSize(record);
      records.add(record);
    }

    boolean hasRoom (byte[] record) {
      long recordLen = EventhubBatch.this.getInternalSize(record);
      return (byteSize + recordLen) <= EventhubBatch.this.memSizeLimit;
    }

    long getByteSize() {
      return byteSize;
    }

    List<byte[]> getRecords() {
      return records;
    }
  }

  public List<byte[]> getRecords() {
    return memory.getRecords();
  }

  public boolean hasRoom(byte[] object) {
    return memory.hasRoom(object);
  }

  public void append(byte[] object) {
     memory.append(object);
  }

  public int getRecordSizeInByte (byte[] record) {
    return record.length;
  }

  public long getCurrentSizeInByte() {
    return memory.getByteSize();
  }
}
