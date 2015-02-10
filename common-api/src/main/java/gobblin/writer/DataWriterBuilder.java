/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.writer;

import java.io.IOException;


/**
 * A builder class for {@link DataWriter}.
 *
 * @param <S> schema type
 * @param <D> data record type
 *
 * @author ynli
 */
public abstract class DataWriterBuilder<S, D> {

  protected Destination destination;
  protected String writerId;
  protected WriterOutputFormat format;
  protected S schema;
  protected int branch;

  /**
   * Tell the writer the destination to write to.
   *
   * @param destination destination to write to
   * @return this {@link DataWriterBuilder} instance
   */
  public DataWriterBuilder<S, D> writeTo(Destination destination) {
    this.destination = destination;
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
    return this;
  }

  /**
   * Build a {@link DataWriter}.
   *
   * @throws IOException if there is anything wrong building the writer
   * @return the built {@link DataWriter}
   */
  @SuppressWarnings("unchecked")
  public abstract DataWriter<D> build()
      throws IOException;
}
