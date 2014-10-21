package com.linkedin.uif.fork;

import org.apache.avro.Schema;

/**
 * A wrapper class for {@link org.apache.avro.Schema} that is also {@link Copyable}.
 *
 * @author ynli
 */
public class CopyableSchema implements Copyable<Schema> {

    private final Schema schema;

    public CopyableSchema(Schema schema) {
        this.schema = schema;
    }

    @Override
    public Schema copy() throws CopyNotSupportedException {
        return new Schema.Parser().parse(this.schema.toString());
    }
}
