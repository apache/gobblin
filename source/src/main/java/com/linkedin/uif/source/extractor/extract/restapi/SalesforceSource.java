package com.linkedin.uif.source.extractor.extract.restapi;

import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.source.extractor.Extractor;
import com.linkedin.uif.source.extractor.extract.BaseSource;

/**
 * An implementation of salesforce source to get work units
 */
public class SalesforceSource<S, D> extends BaseSource<S, D> {

	public Extractor<S, D> getExtractor(WorkUnitState state) {
		return new SalesforceExtractor<S, D>(state);
	}
}
