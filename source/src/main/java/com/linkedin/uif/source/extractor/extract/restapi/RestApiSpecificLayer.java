package com.linkedin.uif.source.extractor.extract.restapi;

import java.util.List;

import org.apache.http.HttpEntity;

import com.linkedin.uif.source.extractor.exception.RestApiConnectionException;
import com.linkedin.uif.source.extractor.extract.Command;

public interface RestApiSpecificLayer {
	public HttpEntity getAuthentication() throws RestApiConnectionException;
	public boolean getPullStatus();
	public String getNextUrl();
}
