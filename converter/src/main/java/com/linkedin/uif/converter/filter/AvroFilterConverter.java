package com.linkedin.uif.converter.filter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData.Record;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.converter.AvroToAvroConverterBase;
import com.linkedin.uif.converter.DataConversionException;
import com.linkedin.uif.converter.SchemaConversionException;

public abstract class AvroFilterConverter extends AvroToAvroConverterBase
{
    @Override
    public Schema convertSchema(Schema inputSchema, WorkUnitState workUnit) throws SchemaConversionException
    {
        return inputSchema;
    }

    @Override
    public GenericRecord convertRecord(Schema outputSchema, GenericRecord inputRecord, WorkUnitState workUnit) throws DataConversionException
    {
        String[] fieldPath = workUnit.getProp(ConfigurationKeys.CONVERTER_FILTER_FIELD).split("\\.");
        HashSet<Object> filterIds = getFilterIds();
        if (filterIds.contains(extractField(inputRecord, fieldPath, 0))) {
            return inputRecord;
        } else {
            return null;
        }
    }
        
    public Object extractField(Object data, String[] fieldPath, int field) {
        if ((field + 1) == fieldPath.length) {
            Object result = (Object) ((Record) data).get(fieldPath[field]);
            if (result == null) {
                return null;
            } else
                return result;
        } else {
            return extractField(((Record) data).get(fieldPath[field]), fieldPath, ++field);
        }
    }
    
    public abstract HashSet<Object> getFilterIds();
}
