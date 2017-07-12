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

package gobblin.writer.initializer;

import lombok.ToString;
import gobblin.initializer.Initializer;
import gobblin.initializer.NoopInitializer;

@ToString
public class NoopWriterInitializer implements WriterInitializer {
  public static final NoopWriterInitializer INSTANCE = new NoopWriterInitializer();

  private final Initializer initializer = NoopInitializer.INSTANCE;

  private NoopWriterInitializer() {}

  @Override
  public void initialize() {
    this.initializer.initialize();
  }

  @Override
  public void close() {
    this.initializer.close();
  }
}
