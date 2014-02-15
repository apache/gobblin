package com.linkedin.uif.scheduler;

import java.io.IOException;

/**
 * An interface for classes that track {@link TaskState}s.
 *
 * @author ynli
 */
public interface TaskStateTracker {

    /**
     * Report the {@link TaskState} of a {@link Task}.
     *
     * @param state {@link TaskState} of a {@link Task}
     * @throws IOException
     */
    public void reportTaskState(TaskState state) throws IOException;
}
