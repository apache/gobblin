package gobblin.metastore.database;

import gobblin.metastore.JobHistoryStore;

import javax.sql.DataSource;

/**
 * Denotes that a class that is a database store.
 */
public interface VersionedDatabaseJobHistoryStore extends JobHistoryStore {
    /**
     * Initializes the datastore
     * @param dataSource The datasource the datastore should connect to.
     */
    void init(DataSource dataSource);
}

