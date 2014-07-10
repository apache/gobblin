package com.linkedin.uif.source.extractor.extract.sftp;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.SftpException;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.SourceState;
import com.linkedin.uif.configuration.State;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.source.extractor.Extractor;
import com.linkedin.uif.source.extractor.extract.Command;
import com.linkedin.uif.source.extractor.extract.CommandOutput;
import com.linkedin.uif.source.extractor.extract.FileBasedSource;
import com.linkedin.uif.source.extractor.extract.sftp.SftpCommand.SftpCommandType;

/**
 * Source class for Responsys data, responsible for querying Responsys
 * in order to get a list of files to pull for this current run. It then
 * distributes the files among the work units
 * @author stakiar
 */
public class ResponsysSource extends FileBasedSource<String, String>
{  
    private static final Logger log = LoggerFactory.getLogger(ResponsysSource.class);
        
    private ChannelSftp sftp;

    @Override
    public Extractor<String, String> getExtractor(WorkUnitState state) throws IOException {
        return new ResponsysExtractor(state);
    }
 
    @Override
    public void shutdown(SourceState state)
    {
        log.info("Shutting down the sftp connection");
        sftp.disconnect();
    }
    
    /**
     * Connects to the source and does on ls on the directory where the data is located,
     * looking for files with the pattern *SOURCE_ENTITY*
     * @return list of file names matching the specified pattern
     */
    @Override
    public List<String> getcurrentFsSnapshot(State state)
    {
        this.sftp = (ChannelSftp) SftpHelper.connect(state.getProp(ConfigurationKeys.SOURCE_CONN_PRIVATE_KEY),
                                                       state.getProp(ConfigurationKeys.SOURCE_CONN_KNOWN_HOSTS),
                                                       state.getProp(ConfigurationKeys.SOURCE_CONN_USERNAME),
                                                       state.getProp(ConfigurationKeys.SOURCE_CONN_HOST_NAME),
                                                       state.getPropAsInt(ConfigurationKeys.SOURCE_CONN_PORT, ConfigurationKeys.SOURCE_CONN_DEFAULT_PORT),
                                                       state.getProp(ConfigurationKeys.SOURCE_CONN_USE_PROXY_URL),
                                                       state.getPropAsInt(ConfigurationKeys.SOURCE_CONN_USE_PROXY_PORT, -1));
        
        List<Command> cmds = Lists.newArrayList();
        List<String> list = Arrays.asList(state.getProp(ConfigurationKeys.SOURCE_FILEBASED_DATA_DIRECTORY) + "/*" + state.getProp(ConfigurationKeys.SOURCE_ENTITY) + "*");
        cmds.add(new SftpCommand().build(list, SftpCommandType.LS));

        CommandOutput<SftpCommand, List<String>> response = new SftpCommandOutput();
        try {
            response = SftpHelper.executeUnixCommands(cmds, this.sftp);
        } catch (SftpException e) {
            throw new RuntimeException(e.getMessage(), e);
        } catch (SftpCommandFormatException e) {
            throw new RuntimeException(e.getMessage(), e);
        }

        Map<SftpCommand, List<String>> results = response.getResults();
        for (Map.Entry<SftpCommand, List<String>> entry : results.entrySet()) {
            if (entry.getKey().getCommandType().equals(SftpCommandType.LS)) {
                List<String> fsSnapshot = entry.getValue();
                for (int i = 0; i < fsSnapshot.size(); i++) {
                    fsSnapshot.set(i, state.getProp(ConfigurationKeys.SOURCE_FILEBASED_DATA_DIRECTORY) + "/" + fsSnapshot.get(i));
                }
                return fsSnapshot;
            }
        }
        return null;
    }
}
