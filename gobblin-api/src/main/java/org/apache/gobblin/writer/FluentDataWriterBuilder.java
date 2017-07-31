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

/**
 * A helper class to help create fluent {@link DataWriterBuilder}s. To make the Java generics magic
 * work, classes should declare their builders as MyDataWriterBuilder<S, D, MyDataWriterBuilder>
 * and use "return {@link #typedSelf()}" instead of "return this" in their setters.
 */
public abstract class FluentDataWriterBuilder<S, D, B extends FluentDataWriterBuilder<S, D, B>>
    extends DataWriterBuilder<S, D> {

  @SuppressWarnings("unchecked")
  protected B typedSelf() {
    return (B)this;
  }

  @Override
  public B writeTo(Destination destination) {
    super.writeTo(destination);
    return typedSelf();
  }

  @Override
  public B writeInFormat(WriterOutputFormat format) {
    super.writeInFormat(format);
    return typedSelf();
  }

  @Override
  public DataWriterBuilder<S, D> withWriterId(String writerId) {
    super.withWriterId(writerId);
    return typedSelf();
  }

  @Override
  public DataWriterBuilder<S, D> withSchema(S schema) {
    super.withSchema(schema);
    return typedSelf();
  }

  @Override
  public DataWriterBuilder<S, D> withBranches(int branches) {
    super.withBranches(branches);
    return typedSelf();
  }

  @Override
  public DataWriterBuilder<S, D> forBranch(int branch) {
    super.forBranch(branch);
    return typedSelf();
  }

}
