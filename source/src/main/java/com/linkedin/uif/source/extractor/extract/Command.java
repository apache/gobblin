package com.linkedin.uif.source.extractor.extract;

import java.util.Collection;
import java.util.List;

/**
 * Interface for a source command (e.g. REST, SFTP, etc.)
 * Specifies a Command along with a CommandType and a list of parameters
 * @author stakiar
 */
public interface Command
{
    public Command withParams(Collection<String> params);
    public List<String> getParams();
    public Command withCommandType(CommandType cmd);
    public CommandType getCommandType();
    public Command build(Collection<String> params, CommandType cmd);
}