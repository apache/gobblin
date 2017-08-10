package org.apache.gobblin.source.extractor.extract.jdbc;

import java.io.IOException;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.source.extractor.exception.ExtractPrepareException;
import org.apache.gobblin.source.extractor.extract.QueryBasedSource;
import org.apache.gobblin.source.jdbc.PostgresqlExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;


/**
 * An implementation of postgresql source to get work units
 *
 * @author tilakpatidar
 */

public class PostgresqlSource extends QueryBasedSource<JsonArray, JsonElement> {
  private static final Logger LOG = LoggerFactory.getLogger(PostgresqlSource.class);

  @Override
  public Extractor<JsonArray, JsonElement> getExtractor(WorkUnitState state)
      throws IOException {
    Extractor<JsonArray, JsonElement> extractor;
    try {
      extractor = new PostgresqlExtractor(state).build();
    } catch (ExtractPrepareException e) {
      LOG.error("Failed to prepare extractor: error - " + e.getMessage());
      throw new IOException(e);
    }
    return extractor;
  }
}