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
package org.apache.gobblin.writer;
import org.apache.gobblin.annotation.Alpha;

import java.util.LinkedList;
import java.util.List;


/**
 * A batch which internally saves each record in memory with bounded size limit
 * Also a TTL value is configured, so that an eviction policy can be applied form upper layer.
 */
@Alpha
public class BytesBoundedBatch<D> extends Batch<D>{
  private RecordMemory memory;
  private final long creationTimestamp;
  private final long memSizeLimit;
  private final long ttlInMilliSeconds;
  public static final int OVERHEAD_SIZE_IN_BYTES = 15;

  public BytesBoundedBatch(long memSizeLimit, long ttlInMilliSeconds) {
    this.creationTimestamp = System.currentTimeMillis();
    this.memory = new RecordMemory();
    this.memSizeLimit = memSizeLimit;
    this.ttlInMilliSeconds = ttlInMilliSeconds;
  }

  public boolean isTTLExpire() {
    return (System.currentTimeMillis() - creationTimestamp) >= ttlInMilliSeconds;
  }

  private long getInternalSize(D record) {
    return (record).toString().length() + this.OVERHEAD_SIZE_IN_BYTES;
  }

  public  class RecordMemory {
    private List<D> records;
    private long byteSize;

    public RecordMemory () {
      byteSize = 0;
      records = new LinkedList<>();
    }

    void append (D record) {
      byteSize += BytesBoundedBatch.this.getInternalSize(record);
      records.add(record);
    }

    boolean hasRoom (D record) {
      long recordLen = BytesBoundedBatch.this.getInternalSize(record);
      return (byteSize + recordLen) <= BytesBoundedBatch.this.memSizeLimit;
    }

    long getByteSize() {
      return byteSize;
    }

    List<D> getRecords() {
      return records;
    }
  }

  public List<D> getRecords() {
    return memory.getRecords();
  }

  public boolean hasRoom (D object) {
    return memory.hasRoom(object);
  }

  public void append (D object) {
     memory.append(object);
  }

  public int getRecordSizeInByte (D record) {
    return (record).toString().length();
  }

  public long getCurrentSizeInByte() {
    return memory.getByteSize();
  }
}
