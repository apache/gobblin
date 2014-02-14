package com.linkedin.uif.source.workunit;

/**
 * An immutable version of {@link WorkUnit}.
 *
 * @author ynli
 */
public class ImmutableWorkUnit extends WorkUnit {

    public ImmutableWorkUnit(WorkUnit workUnit) {
        this.addAll(workUnit);
    }

    @Override
    public void setProp(String key, Object value) {
        throw new UnsupportedOperationException();
    }
}