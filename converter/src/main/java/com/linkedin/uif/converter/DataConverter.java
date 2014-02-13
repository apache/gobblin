package com.linkedin.uif.converter;

import org.apache.avro.generic.GenericRecord;

/**
 * Converter for converting a source data record of a given type
 * to an Avro {@link GenericRecord}.
 *
 * <p>
 *     This is currently only used by the writer.
 * </p>
 *
 * @param <S> type of source data record representation
 * @param <O> target record data type
 *
 * @author ynli
 */
public interface DataConverter<S, O> {

    /**
     * Convert the given source data record into a target data record.
     *
     * @param sourceRecord source data record
     * @return converted target data record
     * @throws DataConversionException when there's anything wrong with the conversion
     */
    public O convert(S sourceRecord) throws DataConversionException;
}
