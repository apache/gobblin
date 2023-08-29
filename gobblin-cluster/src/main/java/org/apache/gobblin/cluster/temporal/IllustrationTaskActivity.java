package org.apache.gobblin.cluster.temporal;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;
/**
 * Activity for processing {@link IllustrationTask}s
 *
 * CAUTION/FINDING: an `@ActivityInterface` must not be parameterized (e.g. here, by TASK), as doing so results in:
 *   io.temporal.failure.ApplicationFailure: message='class java.util.LinkedHashMap cannot be cast to class
 *       com.linkedin.temporal.app.work.IllustrationTask', type='java.lang.ClassCastException'
 */
@ActivityInterface
public interface IllustrationTaskActivity {
    @ActivityMethod
    String doTask(IllustrationTask task);
}