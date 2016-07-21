package gobblin.refactor;

import com.typesafe.config.Config;
import java.net.URI;
import java.util.Properties;


public class JobSpec implements Configurable {

  final URI uri ;
  final String version;
  final String description;
  final URI template;
  final Config config ;

  private static final String DEFAULT_DESCRIBE_INFO = "Normal Job Spec" ;

  public JobSpec( URI uri, String version, Config config ){
    this(uri, version, DEFAULT_DESCRIBE_INFO , null, config) ;
  }
  public JobSpec(URI uri, String version, String description, Config config){
    this(uri, version, description, null, config) ;
  }
  public JobSpec(URI uri, String version, String description, URI template, Config config) {
    this.uri = uri ;
    this.version = version ;
    this.description = description ;
    this.template = template;
    this.config = config ;
  }

  public Config getConfig(){
    return this.config;
  }

  /**
   * todo : Convert the Config into Properties format.
   * @return
   */
  public Properties getConfigAsProperties(){
    Properties convertedProp = new Properties() ;
    return convertedProp ;
  }
}
