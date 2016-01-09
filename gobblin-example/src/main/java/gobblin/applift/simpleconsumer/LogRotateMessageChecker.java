package gobblin.applift.simpleconsumer;

import gobblin.configuration.State;
import gobblin.qualitychecker.row.RowLevelPolicy;

public class LogRotateMessageChecker extends RowLevelPolicy {

	public LogRotateMessageChecker(State state, Type type) {
		super(state, type);
	}

	
	/*
	 * Policy to discard LogRotate message.
	 * @see gobblin.qualitychecker.row.RowLevelPolicy#executePolicy(java.lang.Object)
	 */
	@Override
	public Result executePolicy(Object record) {
		String logRecord = (String)record;
		boolean isLogRotate = logRecord.contains("LOGROTATE");
		if (isLogRotate) {
			return Result.FAILED;
		}
		return Result.PASSED;
	}
}
