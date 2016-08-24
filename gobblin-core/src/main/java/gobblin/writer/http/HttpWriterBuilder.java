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
package gobblin.writer.http;

import gobblin.writer.DataWriter;

import java.io.IOException;

/**
 * Builder that builds HttpWriter
 */
public class HttpWriterBuilder extends AbstractHttpWriterBuilder<Void, String, HttpWriterBuilder>{

  @Override
  public DataWriter<String> build() throws IOException {
    validate();
    return new HttpWriter<String>(this);
  }
}