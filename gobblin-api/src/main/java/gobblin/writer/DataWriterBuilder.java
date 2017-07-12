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

package gobblin.writer;

import java.io.IOException;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import gobblin.configuration.State;
import gobblin.source.workunit.WorkUnitStream;
import gobblin.writer.initializer.NoopWriterInitializer;
import gobblin.writer.initializer.WriterInitializer;


/**
 * A builder class for {@link DataWriter}.
 *
 * @param <S> schema type
 * @param <D> data record type
 *
 * @author Yinan Li
 */
@Getter
@Slf4j
public abstract class DataWriterBuilder<S, D> {

  protected Destination destination;
  protected String writerId;
  protected WriterOutputFormat format;
  protected S schema;
  protected int branches;
  protected int branch;
  protected String writerAttemptId;

  /**
   * Tell the writer the destination to write to.
   *
   * @param destination destination to write to
   * @return this {@link DataWriterBuilder} instance
   */
  public DataWriterBuilder<S, D> writeTo(Destination destination) {
    this.destination = destination;
    log.debug("For destination: {}", destination);
    return this;
  }

  /**
   * Tell the writer the output format of type {@link WriterOutputFormat}.
   *
   * @param format output format of the writer
   * @return this {@link DataWriterBuilder} instance
   */
  public DataWriterBuilder<S, D> writeInFormat(WriterOutputFormat format) {
    this.format = format;
    log.debug("writeInFormat : {}", this.format);
    return this;
  }

  /**
   * Give the writer a unique ID.
   *
   * @param writerId unique writer ID
   * @return this {@link DataWriterBuilder} instance
   */
  public DataWriterBuilder<S, D> withWriterId(String writerId) {
    this.writerId = writerId;
    log.debug("withWriterId : {}", this.writerId);
    return this;
  }

  /**
   * Tell the writer the data schema.
   *
   * @param schema data schema
   * @return this {@link DataWriterBuilder} instance
   */
  public DataWriterBuilder<S, D> withSchema(S schema) {
    this.schema = schema;
    log.debug("withSchema : {}", this.schema);
    return this;
  }

  /**
   * Tell the writer how many branches are being used.
   *
   * @param branches is the number of branches
   * @return this {@link DataWriterBuilder} instance
   */
  public DataWriterBuilder<S, D> withBranches(int branches) {
    this.branches = branches;
    log.debug("With branches: {}", this.branches);
    return this;
  }

  /**
   * Tell the writer which branch it is associated with.
   *
   * @param branch branch index
   * @return this {@link DataWriterBuilder} instance
   */
  public DataWriterBuilder<S, D> forBranch(int branch) {
    this.branch = branch;
    log.debug("For branch: {}", this.branch);
    return this;
  }

  /**
   * Attempt Id for this writer. There could be two duplicate writers with the same {@link #writerId},
   * their writerAttemptId should be different.
   */
  public DataWriterBuilder<S, D> withAttemptId(String attemptId) {
    this.writerAttemptId = attemptId;
    log.debug("With writerAttemptId: {}", this.writerAttemptId);
    return this;
  }

  public WriterInitializer getInitializer(State state, WorkUnitStream workUnits, int branches, int branchId) {
    return NoopWriterInitializer.INSTANCE;
  }

  /**
   * Build a {@link DataWriter}.
   *
   * @throws IOException if there is anything wrong building the writer
   * @return the built {@link DataWriter}
   */
  public abstract DataWriter<D> build() throws IOException;
}
