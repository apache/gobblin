package com.linkedin.uif.extractor.inputsource;

import java.io.Serializable;
import java.util.Properties;

/**
 * A logic unit of work to be scheduled and executed.
 *
 * @param <S>
 * @param <D>
 */
public class WorkUnit<S, D> implements Comparable<WorkUnit<S, D>>, Serializable {

    private final int priority;

    private final Properties properties;

    public WorkUnit(int priority) {
        this.priority = priority;
        this.properties = new Properties();
    }

    @Override
    public int compareTo(WorkUnit workUnit) {
        return this.priority - workUnit.priority;
    }

    /**
     *
     * @return
     */
    public Properties getProperties() {
        return this.properties;
    }

    /**
     *
     * @return
     */
    public S getSourceSchema() {
        return null;
    }
}
