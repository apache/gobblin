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
import java.util.Collections;
import java.util.List;

import com.google.common.base.Throwables;
import org.slf4j.Logger;

import gobblin.configuration.SourceState;
import gobblin.configuration.WorkUnitState;
import gobblin.source.Source;
import gobblin.source.extractor.Extractor;
import gobblin.source.workunit.WorkUnit;


/**
 * A decorator class for {@link gobblin.source.Source} that catches any
 * possible exceptions/errors thrown by the {@link gobblin.source.Source}.
 *
 * @author ynli
 */
public class SourceDecorator<S, D> implements Source<S, D> {

  private final Source<S, D> source;
  private final String jobId;
  private final Logger logger;

  public SourceDecorator(Source<S, D> source, String jobId, Logger logger) {
    this.source = source;
    this.jobId = jobId;
    this.logger = logger;
  }

  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {
    try {
      List<WorkUnit> workUnits = this.source.getWorkunits(state);
      if (workUnits == null) {
        // Return an empty list if no work units are returned by the source
        return Collections.emptyList();
      }
      return workUnits;
    } catch (Throwable t) {
      this.logger.error("Failed to get work units for job " + this.jobId, t);
      // Return null in case of errors
      return null;
    }
  }

  @Override
  public Extractor<S, D> getExtractor(WorkUnitState state)
      throws IOException {
    try {
      return this.source.getExtractor(state);
    } catch (Throwable t) {
      this.logger.error("Failed to get extractor for job " + this.jobId, t);
      Throwables.propagate(t);
      // Dummy return that is not reachable as propagate above throws RuntimeException
      return null;
    }
  }

  @Override
  public void shutdown(SourceState state) {
    try {
      this.source.shutdown(state);
    } catch (Throwable t) {
      this.logger.error("Failed to shutdown source for job " + this.jobId, t);
    }
  }
}
