package com.linkedin.uif.writer.converter;

import com.linkedin.uif.converter.SchemaConversionException;
import org.apache.avro.Schema;

/**
 * Converter for converting a source data schema of a given type
 * to an Avro {@link Schema}.
 *
 * <p>
 *     This is currently only used by the writer.
 * </p>
 *
 * @param <S> type of source schema representation
 * @param <O> target schema type
 *
 * @author ynli
 */
public interface SchemaConverter<S, O> {

    /**
     * Convert the given source data schema into a target schema.
     *
     * @param sourceSchema source data schema
     * @return converted target schema
     * @throws com.linkedin.uif.converter.SchemaConversionException when there's anything wrong with the conversion
     */
    public O convert(S sourceSchema) throws SchemaConversionException;
}
