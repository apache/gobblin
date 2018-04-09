package org.apache.gobblin.codec;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.SnappyCodec;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Implement Snappy compression and decompression in hadoop.
 */

public class HadoopSnappyCodec implements StreamCodec {
    public static final String TAG = "snappy";
    private SnappyCodec codec;

    public HadoopSnappyCodec() {
        Configuration conf = new Configuration();
        codec = new SnappyCodec();
        codec.setConf(conf);
    }

    @Override
    public OutputStream encodeOutputStream(OutputStream origStream)
            throws IOException {
        return codec.createOutputStream(origStream);
    }

    @Override
    public InputStream decodeInputStream(InputStream origStream)
            throws IOException {
        return codec.createInputStream(origStream);
    }

    @Override
    public String getTag() {
        return TAG;
    }
}
