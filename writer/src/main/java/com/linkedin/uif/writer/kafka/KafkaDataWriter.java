package com.linkedin.uif.writer.kafka;

import com.linkedin.uif.writer.DataWriter;
import com.linkedin.uif.writer.converter.DataConverter;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: ynli
 * Date: 1/29/14
 * Time: 5:31 PM
 * To change this template use File | Settings | File Templates.
 */
public class KafkaDataWriter<T> implements DataWriter<T> {

    @Override
    public void write(T sourceRecord) throws IOException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void close() throws IOException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void commit() throws IOException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public long recordsWritten() {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
