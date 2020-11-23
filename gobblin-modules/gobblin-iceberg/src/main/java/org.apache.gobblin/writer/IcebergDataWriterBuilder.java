package org.apache.gobblin.writer;


import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import java.io.IOException;

public class IcebergDataWriterBuilder<S,D> extends FsDataWriterBuilder<S, D> {

    @Override
    public DataWriter<D> build()
            throws IOException {
        Preconditions.checkNotNull(this.destination);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(this.writerId));
        Preconditions.checkNotNull(this.schema);
        Preconditions.checkArgument(this.format == WriterOutputFormat.PARQUET);

        switch (this.destination.getType()) {
            case HDFS:
                return new IcebergWriter<D>(this, this.destination.getProperties());
            default:
                throw new RuntimeException("Unknown destination type: " + this.destination.getType());
        }
    }

}
