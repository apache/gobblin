package org.apache.gobblin.eventhub.source;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.gobblin.configuration.WorkUnitState;

/**
 * Created by Jan on 19-Aug-19.
 */
public class EventHubFromConfigSchema implements EventHubSchema {

    public static final String SCHEMA = EventhubExtractor.CONFIG_PREFIX + "schema";

    private Gson gson = new Gson();

    @Override
    public JsonObject getSchema(WorkUnitState workUnitState) {
        return gson.fromJson(workUnitState.getProp(SCHEMA), JsonObject.class);
    }
}
