package com.linkedin.uif.metastore;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import com.linkedin.gobblin.rest.JobExecutionInfo;
import com.linkedin.gobblin.rest.JobExecutionQuery;

/**
 * An interface for stores that store job execution information.
 *
 * @author ynli
 */
public interface JobHistoryStore extends Closeable {

    /**
     * Insert a new or update an existing job execution record.
     *
     * @param jobExecutionInfo a {@link JobExecutionInfo} record
     * @throws java.io.IOException if the insertion or update fails
     */
    public void put(JobExecutionInfo jobExecutionInfo) throws IOException;

    /**
     * Get a list of {@link JobExecutionInfo} records as results of the given query.
     *
     * @param query a {@link JobExecutionQuery} instance
     * @return a list of {@link JobExecutionInfo} records
     * @throws IOException if the query fails
     */
    public List<JobExecutionInfo> get(JobExecutionQuery query) throws IOException;
}
