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
package org.apache.gobblin.metadata;

import org.apache.gobblin.fork.CopyHelper;
import org.apache.gobblin.fork.CopyNotSupportedException;
import org.apache.gobblin.fork.Copyable;

import com.google.common.base.Optional;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;


/**
 * Global metadata
 * @param <S> schema type
 */
@AllArgsConstructor(access=AccessLevel.PRIVATE)
@EqualsAndHashCode
@Builder
public class GlobalMetadata<S> implements Copyable<GlobalMetadata<S>> {
  @Getter
  private S schema;

  @Override
  public GlobalMetadata<S> copy() throws CopyNotSupportedException {
    if (CopyHelper.isCopyable(schema)) {
      return new GlobalMetadata((S)CopyHelper.copy(schema));
    }

    throw new CopyNotSupportedException("Type is not copyable: " + schema.getClass().getName());
  }

  /**
   * Builder that takes in an input {@GlobalMetadata} to use as a base.
   * @param inputMetadata input metadata
   * @param outputSchema output schema to set in the builder
   * @param <SI> input schema type
   * @param <SO> output schema type
   * @return builder
   */
  public static <SI, SO> GlobalMetadataBuilder<SO> builderWithInput(GlobalMetadata<SI> inputMetadata, Optional<SO> outputSchema) {
    GlobalMetadataBuilder<SO> builder = (GlobalMetadataBuilder<SO>) builder();

    if (outputSchema.isPresent()) {
      builder.schema(outputSchema.get());
    }

    return builder;
  }
}
