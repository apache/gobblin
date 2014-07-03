package com.linkedin.uif.source.extractor.extract.restapi;

import java.io.IOException;

import com.linkedin.uif.source.extractor.extract.QueryBasedSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.source.extractor.Extractor;
import com.linkedin.uif.source.extractor.exception.ExtractPrepareException;

/**
 * An implementation of salesforce source to get work units
 */
public class SalesforceSource extends QueryBasedSource<JsonArray, JsonElement> {
	private static final Logger LOG = LoggerFactory.getLogger(QueryBasedSource.class);

	public Extractor<JsonArray, JsonElement> getExtractor(WorkUnitState state) throws IOException {
		Extractor<JsonArray, JsonElement> extractor = null;
		try {
			extractor = new SalesforceExtractor(state).build();
		} catch (ExtractPrepareException e) {
			LOG.error("Failed to prepare extractor: error - " + e.getMessage());
			throw new IOException(e);
		}
		return extractor;
	}
}
