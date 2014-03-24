package com.linkedin.uif.source.extractor.extract.restapi;

import java.io.IOException;

import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.source.extractor.Extractor;
import com.linkedin.uif.source.extractor.exception.ExtractPrepareException;
import com.linkedin.uif.source.extractor.extract.BaseSource;

/**
 * An implementation of salesforce source to get work units
 */
public class SalesforceSource<S, D> extends BaseSource<S, D> {

	public Extractor<S, D> getExtractor(WorkUnitState state) throws IOException {
		Extractor<S, D> extractor = null;
		try {
			extractor = new SalesforceExtractor<S, D>(state).build();
		} catch (ExtractPrepareException e) {
			e.printStackTrace();
			throw new IOException("Failed to prepare extractor; error-" + e.getMessage());
		}
		return extractor;
	}
}
