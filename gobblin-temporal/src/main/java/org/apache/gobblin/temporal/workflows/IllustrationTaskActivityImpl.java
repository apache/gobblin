package org.apache.gobblin.temporal.workflows;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class IllustrationTaskActivityImpl implements IllustrationTaskActivity {
    @Override
    public String doTask(final IllustrationTask task) {
        log.info("Now performing - '" + task.getName() + "'");
        return task.getName();
    }
}
