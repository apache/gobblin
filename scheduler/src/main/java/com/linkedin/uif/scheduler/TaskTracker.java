package com.linkedin.uif.scheduler;

import java.io.IOException;

/**
 * An interface for classes that track states of tasks.
 *
 * @author ynli
 */
public interface TaskTracker {

    /**
     * Report the {@link TaskState} of a {@link Task}.
     *
     * @param state {@link TaskState} of a {@link Task}
     * @throws IOException
     */
    public void reportTaskState(TaskState state) throws IOException;
}
