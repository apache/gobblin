package com.linkedin.uif.scheduler.mapreduce;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.linkedin.uif.scheduler.JobLock;

/**
 * An implementation of {@link JobLock} that relies on new file creation on HDFS.
 *
 * <p>
 *     Acquiring a lock is done by creating a new empty file on HDFS and releasing
 *     a lock is done by deleting the empty file associated with the lock.
 * </p>
 *
 * <p>
 *     The correctness of this implementation is based on the fact that a new file
 *     can and only can be created once, exaplined below (referenced
 *     http://www.aosabook.org/en/hdfs.html):
 *
 *     <em>
 *         HDFS implements a single-writer, multiple-reader model. The HDFS client
 *         that opens a file for writing is granted a lease for the file; no other
 *         client can write to the file. The writing client periodically renews the
 *         lease by sending a heartbeat to the NameNode. When the file is closed,
 *         the lease is revoked.
 *     </em>
 * </p>
 *
 * @author ynli
 */
public class MRJobLock implements JobLock {

    private static final String LOCK_FILE_EXTENSION = ".lock";

    private final FileSystem fs;
    // Empty file associated with the lock
    private final Path lockFile;

    public MRJobLock(FileSystem fs, String lockFileDir, String jobName)
            throws IOException {

        this.fs = fs;
        this.lockFile = new Path(lockFileDir, jobName + LOCK_FILE_EXTENSION);
    }

    @Override
    public void lock() throws IOException {
        if (!this.fs.createNewFile(this.lockFile)) {
            throw new IOException("Failed to create lock file " + this.lockFile.getName());
        }
    }

    @Override
    public void unlock() throws IOException {
        if (!isLocked()) {
            throw new IOException(
                    String.format("Lock file %s does not exist", this.lockFile.getName()));
        }

        this.fs.delete(this.lockFile, false);
    }

    @Override
    public boolean tryLock() throws IOException {
        return this.fs.createNewFile(this.lockFile);
    }

    @Override
    public boolean isLocked() throws IOException {
        return this.fs.exists(this.lockFile);
    }
}
