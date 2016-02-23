package gobblin.config.regressiontest;

import java.util.ArrayList;
import java.util.List;
public class RawNode {

  private final String ownName;
  private List<RawNode> children = new ArrayList<>();
  
  public RawNode(String name){
    this.ownName = name;
  }
  
  protected void addChild(RawNode child){
    this.children.add(child);
  }
  
  protected RawNode getChild(String childName){
    if(childName == null){
      return null;
    }
    
    for(RawNode child: this.children){
      if(child.getOwnName().equals(childName)){
        return child;
      }
    }
    
    return null;
  }
  
  public String getOwnName(){
    return this.ownName;
  }
  
  protected List<RawNode> getChildren() {
    return this.children;
  }
}
