package gobblin.config.configstore.impl;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigIncludeContext;
import com.typesafe.config.ConfigIncluder;
import com.typesafe.config.ConfigObject;


public class RawConfigIncluder implements ConfigIncluder {
  private List<String> rawIncludes = new ArrayList<String>();
  private final ConfigIncluder fallback;

  public RawConfigIncluder(ConfigIncluder fallback) {
    this.fallback = fallback;
  }

  public List<String> getRawIncludes(){
    return this.rawIncludes;
  }
  
  @Override
  public ConfigIncluder withFallback(ConfigIncluder fallback) {
    if (this == fallback) {
      throw new ConfigException.BugOrBroken("trying to create includer cycle");
    } else if (this.fallback == fallback) {
      return this;
    } else if (this.fallback != null) {
      return new RawConfigIncluder(this.fallback.withFallback(fallback));
    } else {
      return new RawConfigIncluder(fallback);
    }
  }

  @Override
  public ConfigObject include(ConfigIncludeContext context, String name) {
    System.out.println("Name here is " + name);
    this.rawIncludes.add(name);

    System.out.println("added " + name + " size if " + this.rawIncludes.size());
    
    ConfigObject obj = includeWithoutFallback(context, name, this);

//    if(obj==null) {
//      return ConfigFactory.empty().root();
//    }

    // now use the fallback includer if any and merge
    // its result.
    if (fallback != null) {
      return obj.withFallback(fallback.include(context, name));
    } else {
      return obj;
    }
  }

  //the heuristic includer in static form
  static ConfigObject includeWithoutFallback(final ConfigIncludeContext context, String name, RawConfigIncluder raw) {
    // the heuristic is valid URL then URL, else relative to including file;
    // relativeTo in a file falls back to classpath inside relativeTo().

    URL url;
    try {
      url = new URL(name);
    } catch (MalformedURLException e) {
      url = null;
    }

    if (url != null) {
      return includeURLWithoutFallback(context, url);
    } else {
      //TBD
      

      return ConfigFactory.empty().root();
      //NameSource source = new RelativeNameSource(context);
      //return fromBasename(source, name, context.parseOptions());
    }
  }

  static ConfigObject includeURLWithoutFallback(final ConfigIncludeContext context, URL url) {
    return ConfigFactory.parseURL(url, context.parseOptions()).root();
  }


}
