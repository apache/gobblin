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

/**
 * A kafka topic partition.
 * Two partitions are considered equivalent if they have the same topic name and partition id. They may have different leaders.
 *
 * @author ziliu
 *
 */
public final class KafkaPartition {
  private final int id;
  private final String topicName;
  private KafkaLeader leader;

  public static class Builder {
    private int id = 0;
    private String topicName = "";
    private int leaderId = 0;
    private String leaderHost = "";
    private int leaderPort = 0;

    public Builder withId(int id) {
      this.id = id;
      return this;
    }

    public Builder withTopicName(String topicName) {
      this.topicName = topicName;
      return this;
    }

    public Builder withLeaderId(int leaderId) {
      this.leaderId = leaderId;
      return this;
    }

    public Builder withLeaderHost(String leaderHost) {
      this.leaderHost = leaderHost;
      return this;
    }

    public Builder withLeaderPort(int leaderPort) {
      this.leaderPort = leaderPort;
      return this;
    }

    public KafkaPartition build() {
      return new KafkaPartition(this);
    }
  }

  public KafkaPartition(KafkaPartition other) {
    this.topicName = other.topicName;
    this.id = other.id;
    this.leader = new KafkaLeader(other.leader.id, other.leader.host, other.leader.port);
  }

  private KafkaPartition(Builder builder) {
    this.id = builder.id;
    this.topicName = builder.topicName;
    this.leader = new KafkaLeader(builder.leaderId, builder.leaderHost, builder.leaderPort);
  }

  public KafkaLeader getLeader() {
    return leader;
  }

  public String getTopicName() {
    return this.topicName;
  }

  public int getId() {
    return this.id;
  }

  public void setLeader(int leaderId, String leaderHost, int leaderPort) {
    this.leader = new KafkaLeader(leaderId, leaderHost, leaderPort);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + id;
    result = prime * result + ((topicName == null) ? 0 : topicName.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof KafkaPartition)) {
      return false;
    }
    KafkaPartition other = (KafkaPartition) obj;
    if (id != other.id) {
      return false;
    }
    if (topicName == null) {
      if (other.topicName != null) {
        return false;
      }
    } else if (!topicName.equals(other.topicName)) {
      return false;
    }
    return true;
  }

  public final static class KafkaLeader {
    private final int id;
    private final String host;
    private final int port;

    public int getId() {
      return id;
    }

    public String getHost() {
      return host;
    }

    public int getPort() {
      return port;
    }

    public KafkaLeader(int id, String host, int port) {
      this.id = id;
      this.host = host;
      this.port = port;
    }
  }

}
