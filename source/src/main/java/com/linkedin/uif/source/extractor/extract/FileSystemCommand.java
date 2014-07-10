package com.linkedin.uif.source.extractor.extract;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class FileSystemCommand implements Command
{
    
    public enum FileSystemCommandType implements CommandType {
        OPEN
    }
    
    private List<String> params;
    private FileSystemCommandType cmd;
    
    public FileSystemCommand() {
        params = new ArrayList<String>();
    }
    
    @Override
    public List<String> getParams()
    {
        return this.params;
    }

    @Override
    public CommandType getCommandType()
    {
        return this.cmd;
    }

    @Override
    public Command build(Collection<String> params, CommandType cmd)
    {
        this.params.addAll(params);
        this.cmd = (FileSystemCommandType) cmd;
        return this;
    }
}
