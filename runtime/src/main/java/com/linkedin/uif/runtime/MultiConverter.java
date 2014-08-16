package com.linkedin.uif.runtime;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.converter.Converter;
import com.linkedin.uif.converter.DataConversionException;
import com.linkedin.uif.converter.SchemaConversionException;

/**
 * An implementation of {@link Converter} that applies a given list of
 * {@link Converter}s in the given order.
 *
 * @author ynli
 */
public class MultiConverter<SI, DI> extends Converter<SI, Object, DI, Object> {

    // The list of converters to be applied
    private final List<Converter<SI, ?, DI, ?>> converters;
    // Remember the mapping between converter and schema it generates
    private final Map<Converter, Object> schemas = Maps.newHashMap();

    public MultiConverter(List<Converter<SI, ?, DI, ?>> converters) {
        // Make a copy to guard against changes to the converters from outside
        this.converters = Lists.newArrayList(converters);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object convertSchema(SI inputSchema, WorkUnitState workUnit)
            throws SchemaConversionException {

        Object schema = inputSchema;
        for (Converter converter : this.converters) {
            // Apply the converter and remember the output schema of this converter
            schema = converter.convertSchema(schema, workUnit);
            this.schemas.put(converter, schema);
        }

        return schema;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object convertRecord(Object outputSchema, DI inputRecord, WorkUnitState workUnit) 
            throws DataConversionException {

        if (schemas.size() != this.converters.size()) {
            throw new RuntimeException(
                    "convertRecord should be called only after convertSchema is called");
        }

        Object record = inputRecord;
        for (Converter converter : this.converters) {
            // Apply the converter using the output schema of this converter
            record = converter.convertRecord(
                    this.schemas.get(converter), record, workUnit);
        }

        return record;
    }
}
