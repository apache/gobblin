package com.linkedin.uif.converter;

import com.linkedin.uif.source.workunit.WorkUnit;

import java.util.List;

/**
 * An implementation of {@link Converter} that applies a given list of
 * {@link Converter}s in the given order.
 *
 * @author ynli
 */
public class MultiConverter<SI, DI> implements Converter<SI, Object, DI, Object> {

    // The list of converters to be applied
    private final List<Converter<SI, Object, DI, Object>> converters;

    public MultiConverter(List<Converter<SI, Object, DI, Object>> converters) {
        this.converters = converters;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object convertSchema(SI inputSchema, WorkUnit workUnit) {
        Object schema = inputSchema;
        for (Converter converter : this.converters) {
            schema = converter.convertSchema(schema, workUnit);
        }
        return schema;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object convertRecord(Object outputSchema, DI inputRecord, WorkUnit workUnit) {
        Object record = inputRecord;
        for (Converter converter : this.converters) {
            record = converter.convertRecord(outputSchema, record, workUnit);
        }
        return record;
    }
}
