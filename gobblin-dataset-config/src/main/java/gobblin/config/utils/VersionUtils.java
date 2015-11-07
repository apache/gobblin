package gobblin.config.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

public class VersionUtils {
  
  // {v1, V2 ... }
  public static final String VERSION_PATTERN1 = "[Vv]\\d+" ;
  public static final Pattern P1 = Pattern.compile(VERSION_PATTERN1);
  
  // {v1.0, v1.1, V2.1 ...}
  public static final String VERSION_PATTERN2 = "[Vv]\\d+\\.\\d+" ;
  public static final Pattern P2 = Pattern.compile(VERSION_PATTERN2);
  
  // {v1.0.0, v1.1.1, V2.1.2 ...}
  public static final String VERSION_PATTERN3 = "[Vv]\\d+\\.\\d+\\.\\d+" ;
  public static final Pattern P3 = Pattern.compile(VERSION_PATTERN3);
  
  public static Collection<String> getValidVersions(Collection<String> input){
    if(input == null) return null;
    
    List<String> res = new ArrayList<String>();
    for(String s: input){
      if ( P1.matcher(s).matches() || P2.matcher(s).matches() || P3.matcher(s).matches() ){
        res.add(s);
      }
    }
    
    return res;
  }
  
  public static String getCurrentVersion(Collection<String> input){
    if(input == null) return null;
    
    Collection<String> valid = getValidVersions(input);
    if(valid==null || valid.size()==0) return null;
    
    Iterator<String> it = valid.iterator();
    String latest = it.next();
    while(it.hasNext()){
      latest = compare(latest, it.next());
    }

    return latest;
  }
  
  // input should follow the pattern already
  private static String compare(String input1, String input2){
    String v1 = input1.substring(1);
    String v2 = input2.substring(1);
    
    String[] cont1 = v1.split("\\.");
    String[] cont2 = v2.split("\\.");
    int minimal = Math.min(cont1.length, cont2.length);
    for(int i=0; i<minimal; i++){
      if(Integer.parseInt(cont1[i])<Integer.parseInt(cont2[i])){
        return input2;
      }
      else if (Integer.parseInt(cont1[i])>Integer.parseInt(cont2[i])){
        return input1;
      }
    }
    
    // exhaust input2
    if(cont2.length==minimal){
      return input1;
    }
    return input2;
  }
}
