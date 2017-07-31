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

package org.apache.gobblin.initializer;

import java.io.IOException;
import java.util.List;

import lombok.ToString;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;


/**
 * Wraps multiple writer initializer behind its interface. This is useful when there're more than one branch.
 */
@ToString
public class MultiInitializer implements Initializer {
  private final List<Initializer> initializers;
  private final Closer closer;

  public MultiInitializer(List<? extends Initializer> initializers) {
    this.initializers = ImmutableList.copyOf(initializers);
    this.closer = Closer.create();
    for (Initializer initializer : this.initializers) {
      this.closer.register(initializer);
    }
  }

  @Override
  public void initialize() {
    for (Initializer initializer : this.initializers) {
      initializer.initialize();
    }
  }

  @Override
  public void close() {
    try {
      this.closer.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
