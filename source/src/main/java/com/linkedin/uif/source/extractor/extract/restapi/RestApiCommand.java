package com.linkedin.uif.source.extractor.extract.restapi;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.google.common.base.Joiner;
import com.linkedin.uif.source.extractor.extract.Command;
import com.linkedin.uif.source.extractor.extract.CommandType;

public class RestApiCommand implements Command {
    
    public enum RestApiCommandType implements CommandType {
        GET,
        PUT
    }

    private List<String> params;
    private RestApiCommandType cmd;

    public RestApiCommand() {
        this.params = new ArrayList<String>();
    }
    
    @Override
    public Command withParams(Collection<String> params)
    {
        this.params.addAll(params);
        return this;
    }

    @Override
    public List<String> getParams()
    {
        return this.params;
    }

    @Override
    public Command withCommandType(CommandType cmd)
    {
        this.cmd = (RestApiCommandType) cmd;
        return this;
    }

    @Override
    public CommandType getCommandType()
    {
        return this.cmd;
    }

    @Override
    public Command build(Collection<String> params, CommandType cmd)
    {
        return this.withCommandType(cmd).withParams(params);
    }
    
    @Override
    public String toString() {
        Joiner joiner = Joiner.on(":").skipNulls();
        return cmd.toString() + ":" + joiner.join(params);
    }
}
