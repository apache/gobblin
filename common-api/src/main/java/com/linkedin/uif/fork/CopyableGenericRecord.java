package com.linkedin.uif.fork;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/**
 * A wrapper class for {@link org.apache.avro.generic.GenericRecord}
 * that is also {@link Copyable}.
 *
 * @author ynli
 */
public class CopyableGenericRecord implements Copyable<GenericRecord> {

    private final GenericRecord record;

    public CopyableGenericRecord(GenericRecord record) {
        this.record = record;
    }

    @Override
    public GenericRecord copy() throws CopyNotSupportedException {
        if (!(this.record instanceof GenericData.Record)) {
            throw new CopyNotSupportedException(
                    "The record to make copy is not an instance of " + GenericData.Record.class.getName());
        }
        // Make a deep copy of the original record
        return new GenericData.Record((GenericData.Record) this.record, true);
    }
}
