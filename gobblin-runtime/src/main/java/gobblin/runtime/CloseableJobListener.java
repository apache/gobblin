package gobblin.runtime;

import java.io.Closeable;


/**
 * Extension of {@link JobListener} that also extends {@link Closeable}.
 *
 * @see {@link JobListener}
 */
public interface CloseableJobListener extends JobListener, Closeable {

}
