package com.linkedin.uif.qualitychecker.row;

import java.io.Closeable;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

import org.apache.hadoop.fs.Path;

public class RowLevelPolicyChecker implements Closeable {

    private final List<RowLevelPolicy> list;
    private boolean errFileOpen;
    private RowLevelErrFileWriter writer;
        
    public RowLevelPolicyChecker(List<RowLevelPolicy> list) {
        this.list = list;
        this.errFileOpen = false;
        this.writer = new RowLevelErrFileWriter();
    }
        
    public boolean executePolicies(Object record, RowLevelPolicyCheckResults results) throws IOException, URISyntaxException {
        for (RowLevelPolicy p : this.list) {
            RowLevelPolicy.Result result = p.executePolicy(record);
            results.put(p, result);
            
            if (p.getType().equals(RowLevelPolicy.Type.FAIL) && result.equals(RowLevelPolicy.Result.FAILED)) {
                throw new RuntimeException("RowLevelPolicy " + p + " failed on record " + record);
            }
            
            if (p.getType().equals(RowLevelPolicy.Type.ERR_FILE)) {
                if (!errFileOpen) {
                    this.writer.open(new Path(p.getErrFileLocation(), p.toString().replaceAll("\\.", "-") + ".err"));
                    this.writer.write(record);
                } else {
                    this.writer.write(record);
                }
                errFileOpen = true;
                return false;
            }
        }
        return true;
    }

    @Override
    public void close() throws IOException {
        if (errFileOpen) {
            this.writer.close();
        }
    }
}
