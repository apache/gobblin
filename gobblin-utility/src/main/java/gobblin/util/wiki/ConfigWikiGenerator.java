/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */
package gobblin.util.wiki;

import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import lombok.Data;
import lombok.NoArgsConstructor;

import org.apache.commons.lang.StringUtils;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.sun.javadoc.AnnotationDesc;
import com.sun.javadoc.AnnotationDesc.ElementValuePair;
import com.sun.javadoc.ClassDoc;
import com.sun.javadoc.Doclet;
import com.sun.javadoc.FieldDoc;
import com.sun.javadoc.RootDoc;
import com.sun.tools.doclets.standard.Standard;

import gobblin.configuration.ConfigKey;

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateExceptionHandler;

/**
 * A {@link Doclet} that generates custom documentation for gobblin config keys.
 * Scans all fields that are annotated with {@link ConfigKey} and generates documentation as HTML
 */
public class ConfigWikiGenerator extends Standard {

  //Some example keys

  /**
   * Root directory where task state files are stored
   */
  @ConfigKey
  public static final String STATE_STORE_ROOT_DIR_KEY = "state.store.dir";
  /**
   * File system URI for file-system-based task store *
   */
  @ConfigKey
  public static final String STATE_STORE_FS_URI_KEY = "state.store.fs.uri";

  /**
   * Enable / disable state store
   */
  @ConfigKey(def = "false")
  public static final String STATE_STORE_ENABLED = "state.store.enabled";

  /**
   * Set this to only log and not delete data
   */
  @ConfigKey(required = false, def = "false", group = "Retention")
  private static final String RETENTION_SIMULATE_KEY = "gobblin.retention.simulate";

  /**
   * Set his to skip trash and delete data
   */
  @ConfigKey(required = false, def = "false", group = "Retention")
  private static final String RETENTION_SKIP_TRASH_KEY = "gobblin.retention.skiptrash";

  /**
   * Name of the destination db
   */
  @ConfigKey(required = true, group = "Hive Conversion")
  private static final String CONVERSION_DESTINATION_DB_NAME_KEY = "gobblin.conversion.destination.dbName";

  /**
   * Name of the destination table
   */
  @ConfigKey(required = true, group = "Hive Conversion")
  private static final String CONVERSION_DESTINATION_TABLE_NAME_KEY = "gobblin.conversion.destination.tableName";

  /**
   * List of tables to be whitelisted of the form [dbpattern.tablepattern1|tablepattern2|...]
   */
  @ConfigKey(required = true, def = "*.*", group = "Hive Distcp")
  private static final String HIVE_DATASET_FINDER_WHITELIST = "hive.dataset.whitelist";

  /**
   * List of tables to blacklisted of the form [dbpattern.tablepattern1|tablepattern2|...]
   */
  @ConfigKey(required = false, def = "-", group = "Hive Distcp")
  private static final String HIVE_DATASET_FINDER_BLACKLIST = "hive.dataset.blacklist";

  private static final String DOCS_HTML_OUTPUT_PATH = "/tmp/configDocs.html";
  private static final String DOCS_HTML_TEMPLATE_RESOURCE = "configDocs.ftl";

  /**
   * Invoked by the doclet task
   */
  public static boolean start(RootDoc root) {

    Multimap<String, ConfigKeyObj> groupConfigKeys = ArrayListMultimap.create();
    try {

      for (ClassDoc c : root.classes()) {
        // Currently only run for this class
        //if (c.qualifiedTypeName().equals("gobblin.util.wiki.ConfigWikiGenerator")) {
          for (FieldDoc f : c.fields(false)) {
            for (AnnotationDesc annotationDesc : f.annotations()) {
              if (annotationDesc.annotationType().qualifiedName().equals(ConfigKey.class.getName())) {

                ConfigKeyObj obj = new ConfigKeyObj();
                obj.setName(f.constantValue().toString());
                obj.setDoc(f.commentText());
                for (ElementValuePair elementValuePair : annotationDesc.elementValues()) {
                  if (elementValuePair.element().qualifiedName().equals(ConfigKey.class.getName() + ".required")) {
                    obj.setRequired(elementValuePair.value().toString());
                  } else if (elementValuePair.element().qualifiedName().equals(ConfigKey.class.getName() + ".def")) {
                    obj.setDef(StringUtils.remove(elementValuePair.value().toString(), "\""));
                  } else if (elementValuePair.element().qualifiedName().equals(ConfigKey.class.getName() + ".group")) {
                    obj.setGroup(StringUtils.remove(elementValuePair.value().toString(), "\""));
                  }

                }
                groupConfigKeys.put(obj.getGroup(), obj);
              }
            }

          }
        //}
      }

      createHtml(groupConfigKeys, DOCS_HTML_OUTPUT_PATH);
    } catch (Throwable t) {
      System.out.println("Failed to create Config keys doc. Build will still succeed");
      t.printStackTrace();
    }

    return true;
  }

  private static void createHtml(Multimap<String, ConfigKeyObj> groupConfigKeys, String htmlPath) throws Exception {

    Configuration config = new Configuration();

    config.setClassForTemplateLoading(ConfigWikiGenerator.class, "templates");
    config.setDefaultEncoding("UTF-8");
    config.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);

    Map<String, Object> input = new HashMap<String, Object>();

    input.put("title", "Configuration Keys");
    input.put("groupConfigKeys", new TreeMap<>(groupConfigKeys.asMap()));

    Template template = config.getTemplate(DOCS_HTML_TEMPLATE_RESOURCE);

    File html = new File(htmlPath);
    new File(html.getParent()).mkdirs();
    try (Writer fileWriter = new FileWriter(html);) {
      template.process(input, fileWriter);
      System.out.println("Config keys doc written at " + html.getAbsolutePath());
    }
  }

  @Data
  @NoArgsConstructor
  public static class ConfigKeyObj implements Comparable<ConfigKeyObj> {

    private String name;
    private String doc = "";
    private String required = "false";
    private String def = "None";
    private String group = "Gobblin";

    @Override
    public int compareTo(ConfigKeyObj o) {
      return this.name.compareTo(o.name);
    }
  }
}
