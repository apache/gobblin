package com.linkedin.uif.runtime;

import com.google.common.util.concurrent.Service;

/**
 * An interface for classes that track {@link TaskState}s.
 *
 * @author ynli
 */
public interface TaskStateTracker extends Service {

    /**
     * Register a new {@link Task}.
     *
     * @param task {@link Task} to register
     */
    public void registerNewTask(Task task);

    /**
     * Callback method when the {@link Task} is completed.
     *
     * @param task {@link Task} that is completed
     */
    public void onTaskCompletion(Task task);
}
