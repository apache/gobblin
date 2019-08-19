package org.apache.gobblin.eventhub.source;

import com.google.gson.JsonObject;
import org.apache.gobblin.configuration.WorkUnitState;

/**
 * Created by Jan on 19-Aug-19.
 */
public interface EventHubSchema {
    JsonObject getSchema(WorkUnitState workUnitState);
}
