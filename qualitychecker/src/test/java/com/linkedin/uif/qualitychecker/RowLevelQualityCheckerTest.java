package com.linkedin.uif.qualitychecker;

import java.io.File;
import java.net.URI;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.State;
import com.linkedin.uif.qualitychecker.row.RowLevelPolicyCheckResults;
import com.linkedin.uif.qualitychecker.row.RowLevelPolicyChecker;
import com.linkedin.uif.qualitychecker.row.RowLevelPolicyCheckerBuilderFactory;

@Test(groups = {"com.linkedin.uif.qualitychecker"})
public class RowLevelQualityCheckerTest
{
    @Test
    public void testRowLevelPolicy() throws Exception {        
        State state = new State();
        state.setProp(ConfigurationKeys.ROW_LEVEL_POLICY_LIST, "com.linkedin.uif.qualitychecker.TestRowLevelPolicy");
        state.setProp(ConfigurationKeys.ROW_LEVEL_POLICY_LIST_TYPE, "FAIL");

        RowLevelPolicyChecker checker = new RowLevelPolicyCheckerBuilderFactory().newPolicyCheckerBuilder(state).build();    
        RowLevelPolicyCheckResults results = new RowLevelPolicyCheckResults();
        
        FileReader<GenericRecord> fileReader = openFile(state);
        
        for (GenericRecord datum : fileReader) {
            Assert.assertTrue(checker.executePolicies(datum, results));
        }
    }
    
    @Test
    public void testWriteToErrFile() throws Exception {        
        State state = new State();
        state.setProp(ConfigurationKeys.ROW_LEVEL_POLICY_LIST, "com.linkedin.uif.qualitychecker.TestRowLevelPolicyFail");
        state.setProp(ConfigurationKeys.ROW_LEVEL_POLICY_LIST_TYPE, "ERR_FILE");
        state.setProp(ConfigurationKeys.ROW_LEVEL_ERR_FILE, TestConstants.TEST_ERR_FILE);
        state.setProp(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, TestConstants.TEST_FS_URI);

        RowLevelPolicyChecker checker = new RowLevelPolicyCheckerBuilderFactory().newPolicyCheckerBuilder(state).build();    
        RowLevelPolicyCheckResults results = new RowLevelPolicyCheckResults();
        
        FileReader<GenericRecord> fileReader = openFile(state);
        
        for (GenericRecord datum : fileReader) {
            Assert.assertFalse(checker.executePolicies(datum, results));
        }
        
        FileSystem fs = FileSystem.get(new URI(TestConstants.TEST_FS_URI), new Configuration());
        Path outputPath = new Path(TestConstants.TEST_ERR_FILE, state.getProp(ConfigurationKeys.ROW_LEVEL_POLICY_LIST).replaceAll("\\.", "-") + ".err");
        Assert.assertTrue(fs.exists(outputPath));
        fs.delete(outputPath);
    }

    private FileReader<GenericRecord> openFile(State state) throws Exception {
        DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
        FileReader<GenericRecord> fileReader = DataFileReader.openReader(new File(TestConstants.TEST_FILE_NAME), reader);
        return fileReader;
    }
}
