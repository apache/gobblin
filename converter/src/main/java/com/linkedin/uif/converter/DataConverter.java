package com.linkedin.uif.converter;

import org.apache.avro.generic.GenericRecord;

/**
 * Converter for converting a source data record of a given type
 * to an Avro {@link GenericRecord}.
 *
 * @param <D> type of source data record representation
 */
public interface DataConverter<D> {

    /**
     * Convert the given source data record into a {@link GenericRecord}
     *
     * @param sourceRecord source data record
     * @return converted {@link GenericRecord}
     * @throws DataConversionException when there's anything wrong with the conversion
     */
    public GenericRecord convert(D sourceRecord) throws DataConversionException;
}
