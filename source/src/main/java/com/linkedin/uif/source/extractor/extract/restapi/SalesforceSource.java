package com.linkedin.uif.source.extractor.extract.restapi;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.source.extractor.Extractor;
import com.linkedin.uif.source.extractor.exception.ExtractPrepareException;
import com.linkedin.uif.source.extractor.extract.BaseSource;

/**
 * An implementation of salesforce source to get work units
 */
public class SalesforceSource<S, D> extends BaseSource<S, D> {
	private static final Logger LOG = LoggerFactory.getLogger(BaseSource.class);

	public Extractor<S, D> getExtractor(WorkUnitState state) throws IOException {
		Extractor<S, D> extractor = null;
		try {
			extractor = new SalesforceExtractor<S, D>(state).build();
		} catch (ExtractPrepareException e) {
			LOG.error("Failed to prepare extractor: error -" + e.getMessage());
			throw new IOException(e);
		}
		return extractor;
	}
}
