package gobblin.cluster;

import com.google.common.base.Strings;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import java.util.UUID;
import java.util.regex.Pattern;
import org.apache.helix.Criteria;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixProperty;
import org.apache.helix.InstanceType;
import org.apache.helix.messaging.CriteriaEvaluator;
import org.apache.helix.messaging.DefaultMessagingService;
import org.apache.helix.messaging.ZNRecordRow;
import org.apache.helix.PropertyKey;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;


/**
 * #HELIX-0.6.7-WORKAROUND
 * The GobblinHelixMessagingService is a temporary workaround for missing messaging support for INSTANCES in helix 0.6.7
 */
public class GobblinHelixMessagingService extends DefaultMessagingService {

  private GobblinHelixCriteriaEvaluator _gobblinHelixCriteriaEvaluator;
  private HelixManager _manager;

  public GobblinHelixMessagingService(HelixManager manager) {
    super(manager);

    _manager = manager;
    _gobblinHelixCriteriaEvaluator = new GobblinHelixCriteriaEvaluator();
  }

  private List<Message> generateMessagesForController(Message message) {
    List<Message> messages = new ArrayList<Message>();
    String id = UUID.randomUUID().toString();
    Message newMessage = new Message(message.getRecord(), id);
    newMessage.setMsgId(id);
    newMessage.setSrcName(_manager.getInstanceName());
    newMessage.setTgtName("Controller");
    messages.add(newMessage);
    return messages;
  }

  @Override
  public Map<InstanceType, List<Message>> generateMessage(final Criteria recipientCriteria,
      final Message message) {
    Map<InstanceType, List<Message>> messagesToSendMap = new HashMap<InstanceType, List<Message>>();
    InstanceType instanceType = recipientCriteria.getRecipientInstanceType();

    if (instanceType == InstanceType.CONTROLLER) {
      List<Message> messages = generateMessagesForController(message);
      messagesToSendMap.put(InstanceType.CONTROLLER, messages);
      // _dataAccessor.setControllerProperty(PropertyType.MESSAGES,
      // newMessage.getRecord(), CreateMode.PERSISTENT);
    } else if (instanceType == InstanceType.PARTICIPANT) {
      List<Message> messages = new ArrayList<Message>();
      List<Map<String, String>> matchedList =
          _gobblinHelixCriteriaEvaluator.evaluateCriteria(recipientCriteria, _manager);

      if (!matchedList.isEmpty()) {
        Map<String, String> sessionIdMap = new HashMap<String, String>();
        if (recipientCriteria.isSessionSpecific()) {
          HelixDataAccessor accessor = _manager.getHelixDataAccessor();
          PropertyKey.Builder keyBuilder = accessor.keyBuilder();

          List<LiveInstance> liveInstances = accessor.getChildValues(keyBuilder.liveInstances());

          for (LiveInstance liveInstance : liveInstances) {
            sessionIdMap.put(liveInstance.getInstanceName(), liveInstance.getSessionId());
          }
        }
        for (Map<String, String> map : matchedList) {
          String id = UUID.randomUUID().toString();
          Message newMessage = new Message(message.getRecord(), id);
          String srcInstanceName = _manager.getInstanceName();
          String tgtInstanceName = map.get("instanceName");
          // Don't send message to self
          if (recipientCriteria.isSelfExcluded()
              && srcInstanceName.equalsIgnoreCase(tgtInstanceName)) {
            continue;
          }
          newMessage.setSrcName(srcInstanceName);
          newMessage.setTgtName(tgtInstanceName);
          newMessage.setResourceName(map.get("resourceName"));
          newMessage.setPartitionName(map.get("partitionName"));
          if (recipientCriteria.isSessionSpecific()) {
            newMessage.setTgtSessionId(sessionIdMap.get(tgtInstanceName));
          }
          messages.add(newMessage);
        }
        messagesToSendMap.put(InstanceType.PARTICIPANT, messages);
      }
    }
    return messagesToSendMap;
  }

  public static class GobblinHelixCriteriaEvaluator extends CriteriaEvaluator {
    /**
     * Examine persisted data to match wildcards in {@link Criteria}
     * @param recipientCriteria Criteria specifying the message destinations
     * @param manager connection to the persisted data
     * @return map of evaluated criteria
     */
    public List<Map<String, String>> evaluateCriteria(Criteria recipientCriteria, HelixManager manager) {
      // get the data
      HelixDataAccessor accessor = manager.getHelixDataAccessor();
      PropertyKey.Builder keyBuilder = accessor.keyBuilder();

      List<HelixProperty> properties;

      properties = accessor.getChildValues(keyBuilder.liveInstances());

      // flatten the data
      List<ZNRecordRow> allRows = ZNRecordRow.flatten(HelixProperty.convertToList(properties));

      // save the matches
      Set<String> liveParticipants = accessor.getChildValuesMap(keyBuilder.liveInstances()).keySet();
      List<ZNRecordRow> result = Lists.newArrayList();
      for (ZNRecordRow row : allRows) {
        // The participant instance name is stored in the return value of either getRecordId() or getMapSubKey()
        if (rowMatches(recipientCriteria, row) &&
            (liveParticipants.contains(row.getRecordId()) || liveParticipants.contains(row.getMapSubKey()))) {
          result.add(row);
        }
      }

      Set<Map<String, String>> selected = Sets.newHashSet();

      // deduplicate and convert the matches into the required format
      for (ZNRecordRow row : result) {
        Map<String, String> resultRow = new HashMap<String, String>();
        resultRow.put("instanceName", !recipientCriteria.getInstanceName().equals("") ?
            (!Strings.isNullOrEmpty(row.getMapSubKey()) ? row.getMapSubKey() : row.getRecordId()) : "");
        resultRow.put("resourceName", !recipientCriteria.getResource().equals("") ? row.getRecordId() : "");
        resultRow.put("partitionName", !recipientCriteria.getPartition().equals("") ? row.getMapKey() : "");
        resultRow.put("partitionState", !recipientCriteria.getPartitionState().equals("") ? row.getMapValue() : "");
        selected.add(resultRow);
      }

      return Lists.newArrayList(selected);
    }

    /**
     * Check if a given row matches the specified criteria
     * @param criteria the criteria
     * @param row row of currently persisted data
     * @return true if it matches, false otherwise
     */
    private boolean rowMatches(Criteria criteria, ZNRecordRow row) {
      String instanceName = normalizePattern(criteria.getInstanceName());
      String resourceName = normalizePattern(criteria.getResource());
      String partitionName = normalizePattern(criteria.getPartition());
      String partitionState = normalizePattern(criteria.getPartitionState());
      return stringMatches(instanceName, row.getMapSubKey()) && stringMatches(resourceName, row.getRecordId())
          && stringMatches(partitionName, row.getMapKey()) && stringMatches(partitionState, row.getMapValue());
    }

    /**
     * Convert an SQL like expression into a Java matches expression
     * @param pattern SQL like match pattern (i.e. contains '%'s and '_'s)
     * @return Java matches expression (i.e. contains ".*?"s and '.'s)
     */
    private String normalizePattern(String pattern) {
      if (pattern == null || pattern.equals("") || pattern.equals("*")) {
        pattern = "%";
      }
      StringBuilder builder = new StringBuilder();
      for (int i = 0; i < pattern.length(); i++) {
        char ch = pattern.charAt(i);
        if ("[](){}.*+?$^|#\\".indexOf(ch) != -1) {
          // escape any reserved characters
          builder.append("\\");
        }
        // append the character
        builder.append(ch);
      }
      pattern = builder.toString().toLowerCase().replace("_", ".").replace("%", ".*?");
      return pattern;
    }

    /**
     * Check if a string matches a pattern
     * @param pattern pattern allowed by Java regex matching
     * @param value the string to check
     * @return true if they match, false otherwise
     */
    private boolean stringMatches(String pattern, String value) {
      Pattern p = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
      return p.matcher(value).matches();
    }
  }
}
