package com.linkedin.uif.source.extractor.extract.jdbc;

/**
 * Interface for JDBC sources
 * 
 * @author nveeramr
 */
public interface JdbcSpecificLayer {
	/**
	 * Url for JDBC connection
	 * 
	 * @return url
	 */
	public String getConnectionUrl();

	/**
	 * Sample record count specified in input query
	 * 
	 * @param query
	 * @return record count
	 */
	public long exractSampleRecordCountFromQuery(String query);

	/**
	 * Remove sample clause in input query
	 * 
	 * @param query
	 * @return query
	 */
	public String removeSampleClauseFromQuery(String query);

	/**
	 * Remove sample clause
	 * 
	 * @return sample clause
	 */
	public String constructSampleClause();
}
