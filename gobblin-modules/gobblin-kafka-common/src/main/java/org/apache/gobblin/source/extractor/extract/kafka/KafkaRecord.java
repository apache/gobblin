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

package org.apache.gobblin.source.extractor.extract.kafka;

import lombok.Getter;


@Getter
public class KafkaRecord implements Comparable<KafkaRecord> {

  private final Long offset;
  private final String key;
  private final String payload;

  public KafkaRecord(long offset, String key, String payload) {
    super();
    this.offset = offset;
    this.key = key;
    this.payload = payload;
  }

  @Override
  public String toString() {
    return "KafkaRecord [offset=" + this.offset + ", key=" + this.key + ", payload=" + this.payload + "]";
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((this.key == null) ? 0 : this.key.hashCode());
    result = prime * result + (int) (this.offset ^ (this.offset >>> 32));
    result = prime * result + ((this.payload == null) ? 0 : this.payload.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    KafkaRecord other = (KafkaRecord) obj;
    if (this.key == null) {
      if (other.key != null) {
        return false;
      }
    } else if (!this.key.equals(other.key)) {
      return false;
    }
    if (!this.offset.equals(other.offset)) {
      return false;
    }
    if (this.payload == null) {
      if (other.payload != null) {
        return false;
      }
    } else if (!this.payload.equals(other.payload)) {
      return false;
    }
    return true;
  }

  @Override
  public int compareTo(KafkaRecord o) {
    return this.offset.compareTo(o.offset);
  }

}
