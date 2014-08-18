package com.linkedin.uif.converter.filter;

import java.util.HashSet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData.Record;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.converter.AvroToAvroConverterBase;
import com.linkedin.uif.converter.Converter;
import com.linkedin.uif.converter.DataConversionException;
import com.linkedin.uif.converter.SchemaConversionException;

public class AvroFilterConverter extends AvroToAvroConverterBase
{
    private static final Logger log = LoggerFactory.getLogger(AvroFilterConverter.class);
    private String[] fieldPath;
    private HashSet<String> filterIds;
    
    @Override
    public Converter<Schema, Schema, GenericRecord, GenericRecord> init(WorkUnitState workUnit) {
      fieldPath = workUnit.getProp(ConfigurationKeys.CONVERTER_FILTER_FIELD).split("\\.");
      filterIds = new HashSet<String>(workUnit.getPropAsList(ConfigurationKeys.CONVERTER_FILTER_IDS));
      return super.init(workUnit);
    }
    
    @Override
    public Schema convertSchema(Schema inputSchema, WorkUnitState workUnit) throws SchemaConversionException
    {
        return inputSchema;
    }

    @Override
    public GenericRecord convertRecord(Schema outputSchema, GenericRecord inputRecord, WorkUnitState workUnit) throws DataConversionException
    {
        if (filterIds.contains(extractField(inputRecord, fieldPath, 0))) {
            log.info("Dropping record: " + inputRecord);
            return null;
        } else {
            return inputRecord;
        }
    }
    
    /**
     * This method will only work with nested fields, it won't work for arrays or maps
     * @param data
     * @param fieldPath
     * @param field
     * @return
     */
    public String extractField(Object data, String[] fieldPath, int field) {
        if ((field + 1) == fieldPath.length) {
            String result = String.valueOf(((Record) data).get(fieldPath[field]));
            if (result == null) {
                return null;
            } else
                return result;
        } else {
            return extractField(((Record) data).get(fieldPath[field]), fieldPath, ++field);
        }
    }
}
