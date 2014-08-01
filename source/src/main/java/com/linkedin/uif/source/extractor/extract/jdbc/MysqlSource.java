package com.linkedin.uif.source.extractor.extract.jdbc;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.source.extractor.Extractor;
import com.linkedin.uif.source.extractor.exception.ExtractPrepareException;
import com.linkedin.uif.source.extractor.extract.QueryBasedSource;

/**
 * An implementation of mysql source to get work units
 * 
 * @author nveeramr
 */
public class MysqlSource extends QueryBasedSource<JsonArray, JsonElement> {
	private static final Logger LOG = LoggerFactory.getLogger(MysqlSource.class);

	public Extractor<JsonArray, JsonElement> getExtractor(WorkUnitState state) throws IOException {
		Extractor<JsonArray, JsonElement> extractor = null;
		try {
			extractor = new MysqlExtractor(state).build();
		} catch (ExtractPrepareException e) {
			LOG.error("Failed to prepare extractor: error - " + e.getMessage());
			throw new IOException(e);
		}
		return extractor;
	}
}
