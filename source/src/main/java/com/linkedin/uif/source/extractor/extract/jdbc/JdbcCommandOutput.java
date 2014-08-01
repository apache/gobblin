package com.linkedin.uif.source.extractor.extract.jdbc;

import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

import com.linkedin.uif.source.extractor.extract.CommandOutput;

/**
 * Captures output of a JDBC command, keep track of commands and its outputs
 * 
 * @author nveeramr
 */
public class JdbcCommandOutput implements CommandOutput<JdbcCommand, ResultSet> {
	private Map<JdbcCommand, ResultSet> results;

	public JdbcCommandOutput() {
		results = new HashMap<JdbcCommand, ResultSet>();
	}

	@Override
	public void storeResults(Map<JdbcCommand, ResultSet> results) {
		this.results = results;
	}

	@Override
	public Map<JdbcCommand, ResultSet> getResults() {
		return results;
	}

	@Override
	public void put(JdbcCommand key, ResultSet value) {
		results.put(key, value);
	}
}