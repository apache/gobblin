package com.linkedin.uif.source.extractor.extract.jdbc;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.google.common.base.Joiner;
import com.linkedin.uif.source.extractor.extract.Command;
import com.linkedin.uif.source.extractor.extract.CommandType;

/**
 * JDBC command with command type and parameters to execute a command
 * 
 * @author nveeramr
 */
public class JdbcCommand implements Command {

	/**
	 * JDBC command types
	 */
	public enum JdbcCommandType implements CommandType {
		QUERY, QUERYPARAMS, FETCHSIZE, DELETE, UPDATE, SELECT
	}

	private List<String> params;
	private JdbcCommandType cmd;

	public JdbcCommand() {
		this.params = new ArrayList<String>();
	}

	@Override
	public List<String> getParams() {
		return this.params;
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
		this.params.addAll(params);
		this.cmd = (JdbcCommandType) cmd;
		return this;
	}
}