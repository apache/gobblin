package com.linkedin.uif.source.extractor.extract.restapi;

import org.apache.http.HttpEntity;
import com.linkedin.uif.source.extractor.exception.RestApiConnectionException;

public interface RestApiSpecificLayer {
	public HttpEntity getAuthentication() throws RestApiConnectionException;
	public boolean getPullStatus();
	public String getNextUrl();
}
