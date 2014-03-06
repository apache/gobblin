package com.linkedin.uif.test;

import java.util.List;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.linkedin.uif.configuration.SourceState;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.source.Source;
import com.linkedin.uif.source.extractor.Extractor;
import com.linkedin.uif.source.workunit.Extract;
import com.linkedin.uif.source.workunit.Extract.TableType;
import com.linkedin.uif.source.workunit.WorkUnit;

/**
 * An implementation of {@link Source} for integration test.
 *
 * @author ynli
 */
public class TestSource implements Source<String, String> {

    private static final String SOURCE_FILE_LIST_KEY = "source.files";
    private static final String SOURCE_FILE_KEY = "source.file";

    private static final Splitter SPLITTER = Splitter.on(",")
            .omitEmptyStrings()
            .trimResults();

    @Override
    public List<WorkUnit> getWorkunits(SourceState state) {
        // For now we assume we pull only one table - so only one extract object
        Extract extract = new Extract(state, TableType.SNAPSHOT_ONLY, "com.linkedin.uif.test", "TestTable", "1");
        
        String sourceFileList = state.getProp(SOURCE_FILE_LIST_KEY);
        List<WorkUnit> workUnits = Lists.newArrayList();
        
        for (String sourceFile : SPLITTER.split(sourceFileList)) {
            WorkUnit workUnit = new WorkUnit(state, extract);
            workUnit.setProp(SOURCE_FILE_KEY, sourceFile);
            workUnits.add(workUnit);
        }

        return workUnits;
    }

    @Override
    public Extractor<String, String> getExtractor(WorkUnitState state) {
        return new TestExtractor(state);
    }

    @Override
    public void shutdown(SourceState state)
    {
        // Do nothing
    }
}
