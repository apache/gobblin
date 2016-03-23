/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.writer.initializer;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.io.Closer;

/**
 * Wraps multiple writer initializer behind its interface. This is useful when there're more than one branch.
 */
public class MultiWriterInitializer implements WriterInitializer {

  private final List<WriterInitializer> writerInitializers;
  private final Closer closer;

  public MultiWriterInitializer(List<WriterInitializer> writerInitializers) {
    this.writerInitializers = Lists.newArrayList(writerInitializers);
    this.closer = Closer.create();
    for (WriterInitializer wi : this.writerInitializers) {
      closer.register(wi);
    }
  }

  @Override
  public void initialize() {
    for (WriterInitializer wi : writerInitializers) {
      wi.initialize();
    }
  }

  @Override
  public void close() {
    try {
      closer.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String toString() {
    return String.format("MultiWriterInitializer [writerInitializers=%s]", writerInitializers);
  }
}
