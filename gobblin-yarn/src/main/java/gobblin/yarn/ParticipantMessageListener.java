package gobblin.yarn;

import java.util.List;
import org.apache.helix.MessageListener;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;


/**
 * Created by ynli on 8/14/15.
 */
public class ParticipantMessageListener implements MessageListener {

  @Override
  public void onMessage(String instanceName, List<Message> messages, NotificationContext changeContext) {
  }
}
