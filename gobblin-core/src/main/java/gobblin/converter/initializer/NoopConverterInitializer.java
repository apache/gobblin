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

package gobblin.converter.initializer;

import lombok.ToString;
import gobblin.initializer.Initializer;
import gobblin.initializer.NoopInitializer;


@ToString
public class NoopConverterInitializer implements ConverterInitializer {
  private final Initializer initializer = new NoopInitializer();

  @Override
  public void initialize() {
    this.initializer.initialize();
  }

  @Override
  public void close() {
    this.initializer.close();
  }

}
