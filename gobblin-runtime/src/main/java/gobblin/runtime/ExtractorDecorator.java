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

package gobblin.runtime;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;

import com.google.common.base.Throwables;

import gobblin.source.extractor.DataRecordException;
import gobblin.source.extractor.Extractor;
import gobblin.util.Decorator;
import gobblin.util.DecoratorUtils;


/**
 * A decorator class for {@link gobblin.source.extractor.Extractor} that catches any
 * possible exceptions/errors thrown by the {@link gobblin.source.extractor.Extractor}.
 *
 * @author ynli
 */
public class ExtractorDecorator<S, D> implements Extractor<S, D>, Decorator {

  private final Extractor<S, D> extractor;
  private final String taskId;
  private final Logger logger;

  public ExtractorDecorator(Extractor<S, D> extractor, String taskId, Logger logger) {
    this.extractor = extractor;
    this.taskId = taskId;
    this.logger = logger;
  }

  @Override
  public S getSchema() throws IOException {
    try {
      return this.extractor.getSchema();
    } catch (Throwable t) {
      this.logger.error("Failed to get schema for task " + this.taskId, t);
      Throwables.propagate(t);
      // Dummy return that is not reachable as propagate above throws RuntimeException
      return null;
    }
  }

  @Override
  public D readRecord(@Deprecated D reuse) throws DataRecordException, IOException {
    try {
      return this.extractor.readRecord(reuse);
    } catch (Throwable t) {
      this.logger.error("Failed to get data record for task " + this.taskId, t);
      Throwables.propagate(t);
      // Dummy return that is not reachable as propagate above throws RuntimeException
      return null;
    }
  }

  @Override
  public void close() throws IOException {
    this.extractor.close();
  }

  @Override
  public long getExpectedRecordCount() {
    try {
      return this.extractor.getExpectedRecordCount();
    } catch (Throwable t) {
      this.logger.error("Failed to get expected record count for task " + this.taskId, t);
      Throwables.propagate(t);
      // Dummy return that is not reachable as propagate above throws RuntimeException
      return 0;
    }
  }

  @Override
  public long getHighWatermark() {
    try {
      return this.extractor.getHighWatermark();
    } catch (Throwable t) {
      this.logger.error("Failed to get high watermark for task " + this.taskId, t);
      Throwables.propagate(t);
      // Dummy return that is not reachable as propagate above throws RuntimeException
      return 0;
    }
  }

  @Override
  public Object getUnderlying() {
    return DecoratorUtils.resolveUnderlyingObject(this.extractor);
  }

  @Override
  public List<Object> getDecoratorLineage() {
    return DecoratorUtils.resolveDecoratorLineage(this, this.extractor);
  }
}
