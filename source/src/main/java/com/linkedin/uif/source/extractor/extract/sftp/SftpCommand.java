package com.linkedin.uif.source.extractor.extract.sftp;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.google.common.base.Joiner;
import com.linkedin.uif.source.extractor.extract.Command;
import com.linkedin.uif.source.extractor.extract.CommandType;

/**
 * Represents a SftpCommand
 * Contains the command type
 * and any parameters necessary
 * to execute the command
 * @author stakiar
 */
public class SftpCommand implements Command {
    
    /**
     * Enum which lists the CommandTypes
     * supported by the SftpCommand class
     * @author stakiar
     */
    public enum SftpCommandType implements CommandType {
        CD,
        CHMOD,
        LS,
        MKDIR,
        PUT,
        PWD,
        RENAME,
        RM,
        GET_FILE,
        GET_STREAM
    }
    
    private List<String> params;
    private SftpCommandType cmd;
    
    public SftpCommand() {
        this.params = new ArrayList<String>();
    }
    
    @Override
    public Command withParams(Collection<String> params) {
        this.params.addAll(params);
        return this;
    }
    
    @Override
    public List<String> getParams() {
        return this.params;
    }
    
    @Override
    public Command withCommandType(CommandType cmd) {
        this.cmd = (SftpCommandType) cmd;
        return this;
    }
    
    @Override
    public CommandType getCommandType() {
        return this.cmd;
    }
    
    @Override
    public String toString() {
        Joiner joiner = Joiner.on(":").skipNulls();
        return cmd.toString() + ":" + joiner.join(params);
    }

    @Override
    public Command build(Collection<String> params, CommandType cmd) {
        return this.withCommandType(cmd).withParams(params);
    }
}