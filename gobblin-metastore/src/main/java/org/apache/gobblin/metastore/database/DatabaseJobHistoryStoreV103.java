package org.apache.gobblin.metastore.database;

import org.apache.gobblin.metastore.JobHistoryStore;

/**
 * An implementation of {@link JobHistoryStore} backed by MySQL.
 *
 * <p>
 *     The DDLs for the MySQL job history store can be found under metastore/src/main/resources.
 * </p>
 *
 */
@SupportedDatabaseVersion(isDefault = false, version = "1.0.3")
public class DatabaseJobHistoryStoreV103 extends DatabaseJobHistoryStoreV101 implements VersionedDatabaseJobHistoryStore {

}
