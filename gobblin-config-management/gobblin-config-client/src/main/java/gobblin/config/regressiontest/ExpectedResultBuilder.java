package gobblin.config.regressiontest;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import gobblin.config.common.impl.SingleLinkedListConfigKeyPath;
import gobblin.config.store.api.ConfigKeyPath;

import com.google.common.base.Preconditions;

public class ExpectedResultBuilder {

  public static final String EXPECTED_RESULT_FILE = "expected.conf";
  private final FileSystem fs;
  private final URI rootURI;
  private RawNode rootNode;
  private final Map<ConfigKeyPath, SingleNodeExpectedResult> cachedResult = new HashMap<>();
  
  public ExpectedResultBuilder (URI rootURI) throws IOException{
    this.fs = FileSystem.get(rootURI, new Configuration()); 
    this.rootURI = rootURI;
    this.buildResult();
  }
  
  protected void buildResult() throws IOException{
    Path rootPath = new Path(this.rootURI);
    Preconditions.checkState(this.fs.exists(rootPath));
    
    rootNode = new RawNode("");
    FileStatus[] childrenFileStatus = this.fs.listStatus(rootPath);
    buildResultHelper(rootNode, childrenFileStatus, SingleLinkedListConfigKeyPath.ROOT);
  }
  
  private void buildResultHelper(RawNode rawNode, FileStatus[] childrenFileStatus, ConfigKeyPath configKeyPath) throws IOException{
    
    for(FileStatus f: childrenFileStatus){
      // new node
      if(f.isDir()){
        String childName = f.getPath().getName();
        RawNode childRawNode = new RawNode(childName);
        rawNode.addChild(childRawNode);
        ConfigKeyPath childConfigKeyPath = configKeyPath.createChild(childName);
        FileStatus[] nextLevelFileStatus = fs.listStatus(f.getPath());
        this.buildResultHelper(childRawNode, nextLevelFileStatus, childConfigKeyPath);
      }
      // build expected result for current node
      else if (f.getPath().getName().equals(EXPECTED_RESULT_FILE)){
        Reader r = new InputStreamReader(this.fs.open(f.getPath()));
        SingleNodeExpectedResult expected = new SingleNodeExpectedResult(r);
        this.cachedResult.put(configKeyPath, expected);
        r.close();
      }
    }
  }
  
  public SingleNodeExpectedResultIntf getSingleNodeExpectedResult (ConfigKeyPath configKeyPath){
    if(this.cachedResult.containsKey(configKeyPath)){
      return this.cachedResult.get(configKeyPath);
    }
    
    return new EmptySingleNodeExpectedResult();
  }
  
  public List<ConfigKeyPath> getChildren(ConfigKeyPath configKeyPath){
    List<ConfigKeyPath> result = new ArrayList<>();
    if(configKeyPath.isRootPath()){
      for(RawNode r: this.rootNode.getChildren()){
        result.add(configKeyPath.createChild(r.getOwnName()));
      }
      return result;
    }
    
    String absPath = configKeyPath.getAbsolutePathString();
    String[] content = absPath.split("/");
    
    RawNode rawNode = getRawNode(content, this.rootNode, 0);
    if(rawNode == null){
      return result;
    }
    
    for(RawNode r: rawNode.getChildren()){
      result.add(configKeyPath.createChild(r.getOwnName()));
    }
    return result;
  }
  
  private RawNode getRawNode(String[] configKeyPath_absPath, RawNode rawNode, int index){
    if(configKeyPath_absPath[index].equals(rawNode.getOwnName())){
      if(index == configKeyPath_absPath.length-1){
        return rawNode;
      }
      
      index++;
      RawNode child = rawNode.getChild(configKeyPath_absPath[index]);
      if(child == null){
        return null;
      }
      
      return getRawNode(configKeyPath_absPath, child, index);
    }
    
    return null;
  }
}
