package gobblin.applift.simpleconsumer;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.Converter;
import gobblin.converter.DataConversionException;
import gobblin.converter.SchemaConversionException;
import gobblin.converter.SingleRecordIterable;

public class LogConverter extends Converter<Object, Object, String, String> {

	private static final Logger LOG = LoggerFactory.getLogger(LogConverter.class);
	@Override
	public Object convertSchema(Object inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
		return inputSchema;
	}

	@Override
	public Iterable<String> convertRecord(Object outputSchema, String inputRecord, WorkUnitState workUnit)
	    throws DataConversionException {
		if(inputRecord.contains("LOGROTATE"))
  		return new SingleRecordIterable<String>(inputRecord);
  	String[] logRecords = inputRecord.split("\n");
  	List<String> normalizedRecords = new ArrayList<String>();
  	for(String logRecord:logRecords){
  		normalizedRecords.add(normalizeString(logRecord));
  	}
    return normalizedRecords;
	}
	
	public static String normalizeString(String str) {
    if (str != null && !str.equals("") && str.contains("\\x")) {
      StringBuffer buff = new StringBuffer();
      int n = str.length();
      for (int i = 0; i < n; i++) {
        if (i + 3 < n && str.charAt(i) == '\\' && str.charAt(i + 1) == 'x') {
          buff.append(str.charAt(i + 2));
          buff.append(str.charAt(i + 3));
          i = i + 3;
        } else {
          buff.append(Integer.toHexString(str.charAt(i)));
        }
      }
      try {
        String buffString = buff.toString();
        if (buffString == null || buffString.equals("")) {
          return "";
        }
        byte[] bytes = Hex.decodeHex(buffString.toCharArray());
        return new String(bytes, "UTF-8");
      } catch (UnsupportedEncodingException | DecoderException e) {
        LOG.error("Exception while decoding: " + e);
      }
    }
    return str;
  }

}
