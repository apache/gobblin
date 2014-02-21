package com.linkedin.uif.source.workunit;

/**
 * An immutable version of {@link WorkUnit}.
 *
 * @author ynli
 */
public class ImmutableWorkUnit extends WorkUnit {

    public ImmutableWorkUnit(WorkUnit workUnit) {
        this.addAll(workUnit);
        super.setNamespace(workUnit.getNamespace());
        super.setTable(workUnit.getTable());
    }

    @Override
    public void setProp(String key, Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setNamespace(String namespace) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTable(String table) {
        throw new UnsupportedOperationException();
    }
}