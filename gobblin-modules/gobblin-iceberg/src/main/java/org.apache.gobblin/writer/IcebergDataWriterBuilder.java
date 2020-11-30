package org.apache.gobblin.writer;


import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import java.io.IOException;

public class IcebergDataWriterBuilder extends FsDataWriterBuilder<Schema, FileFormat> {

    @Override
    public DataWriter<FileFormat> build()
            throws IOException {
        Preconditions.checkNotNull(this.destination);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(this.writerId));
        Preconditions.checkNotNull(this.schema);
        Preconditions.checkArgument(this.format == WriterOutputFormat.PARQUET);

        switch (this.destination.getType()) {
            case HDFS:
                return new IcebergWriter<FileFormat>(this, this.destination.getProperties());
            default:
                throw new RuntimeException("Unknown destination type: " + this.destination.getType());
        }
    }

}
