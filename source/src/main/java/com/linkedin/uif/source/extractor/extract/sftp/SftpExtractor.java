package com.linkedin.uif.source.extractor.extract.sftp;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.jcraft.jsch.ChannelSftp;
import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.source.extractor.extract.FileBasedExtractor;
import com.linkedin.uif.source.extractor.extract.Command;
import com.linkedin.uif.source.extractor.extract.CommandOutput;

/**
 * Abstract class that implements the SFTP
 * protocol for connecting to source
 * and downloading files
 * @author stakiar
 *
 * @param <D> type of data record
 * @param <S> type of schema
 */
public abstract class SftpExtractor<K, V> extends FileBasedExtractor<K, V, SftpCommand, List<String>>
{
    private static final Logger log = LoggerFactory.getLogger(SftpExtractor.class);
    
    protected ChannelSftp sftp;
    protected SftpHelper executor;
    
    public SftpExtractor(WorkUnitState workUnitState)
    {
        super(workUnitState);
        this.sftp = createSftpConnection();
        this.executor = new SftpHelper();
    }
    
    /**
     * Initializes the ChannelSftp
     * @return the ChannelSftp connection
     */
    public ChannelSftp createSftpConnection() {
        ChannelSftp sftp = (ChannelSftp) SftpHelper.connect(this.workUnitState.getProp(ConfigurationKeys.SOURCE_CONN_PRIVATE_KEY),
                                                              this.workUnitState.getProp(ConfigurationKeys.SOURCE_CONN_KNOWN_HOSTS),
                                                              this.workUnitState.getProp(ConfigurationKeys.SOURCE_CONN_USERNAME),
                                                              this.workUnitState.getProp(ConfigurationKeys.SOURCE_CONN_HOST_NAME),
                                                              this.workUnitState.getPropAsInt(ConfigurationKeys.SOURCE_CONN_PORT, ConfigurationKeys.SOURCE_CONN_DEFAULT_PORT),
                                                              this.workUnitState.getProp(ConfigurationKeys.SOURCE_CONN_USE_PROXY_URL),
                                                              this.workUnitState.getPropAsInt(ConfigurationKeys.SOURCE_CONN_USE_PROXY_PORT, -1));
        return sftp;
    }

    @Override
    public CommandOutput<SftpCommand, List<String>> executeCommands(List<Command> cmds)
    {
        try {
            return SftpHelper.executeUnixCommands(cmds, sftp);
        } catch (Throwable t) {
            Throwables.propagate(t);
            return null;
        }
    }
    
    @Override
    public void close()
    {
        log.info("Shutting down the sftp connection");
        this.sftp.disconnect();
    }
}
