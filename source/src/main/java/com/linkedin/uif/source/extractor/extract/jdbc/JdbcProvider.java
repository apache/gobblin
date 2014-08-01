package com.linkedin.uif.source.extractor.extract.jdbc;

import org.apache.commons.dbcp.BasicDataSource;

/**
 * Create JDBC data source
 * 
 * @author nveeramr
 */
public class JdbcProvider extends BasicDataSource {
	// If extract type is not provided then consider it as a default type
	public JdbcProvider(String driver, String connectionUrl, String user, String password, int numconn, int timeout) {
		this.connect(driver, connectionUrl, user, password, numconn, timeout, "DEFAULT");
	}

	public JdbcProvider(String driver, String connectionUrl, String user, String password, int numconn, int timeout, String type) {
		this.connect(driver, connectionUrl, user, password, numconn, timeout, type);
	}

	public void connect(String driver, String connectionUrl, String user, String password, int numconn, int timeout, String type) {
		this.setDriverClassName(driver);
		this.setUsername(user);
		this.setPassword(password);
		this.setUrl(connectionUrl);
		this.setInitialSize(0);
		this.setMaxIdle(numconn);
		this.setMaxActive(timeout);
	}
}