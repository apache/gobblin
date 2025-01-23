package org.apache.gobblin.temporal.ddm.activity;

public enum ActivityType {
  GENERATE_WORKUNITS,
  RECOMMEND_SCALING,
  DELETE_WORK_DIRS,
  PROCESS_WORKUNIT,
  COMMIT,
  DEFAULT_ACTIVITY // Just a default placeholder activity type
}
