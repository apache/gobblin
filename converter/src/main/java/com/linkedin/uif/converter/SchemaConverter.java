package com.linkedin.uif.converter;

import org.apache.avro.Schema;

/**
 * Converter for converting a source data schema of a given type
 * to an Avro {@link Schema}.
 *
 * @param <S> type of source schema representation
 */
public interface SchemaConverter<S> {

    /**
     * Convert the given source data schema into a {@link Schema}.
     *
     * @param sourceSchema source data schema
     * @return converted {@link Schema}
     * @throws SchemaConversionException when there's anything wrong with the conversion
     */
    public Schema convert(S sourceSchema) throws SchemaConversionException;
}
