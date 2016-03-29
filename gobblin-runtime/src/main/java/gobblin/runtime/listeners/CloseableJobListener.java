package gobblin.runtime.listeners;

import java.io.Closeable;


/**
 * Extension of {@link JobListener} that also extends {@link Closeable}.
 *
 * @see JobListener
 */
public interface CloseableJobListener extends JobListener, Closeable {

}
