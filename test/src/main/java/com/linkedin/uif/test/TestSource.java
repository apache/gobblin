package com.linkedin.uif.test;

import java.util.List;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.SourceState;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.source.Source;
import com.linkedin.uif.source.extractor.Extractor;
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
        String sourceFileList = state.getProp(SOURCE_FILE_LIST_KEY);
        List<WorkUnit> workUnits = Lists.newArrayList();
        for (String sourceFile : SPLITTER.split(sourceFileList)) {
            WorkUnit workUnit = new WorkUnit(null, null);
            workUnit.addAll(state);
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
    public void publishSourceMeta(SourceState state) {

    }
}
