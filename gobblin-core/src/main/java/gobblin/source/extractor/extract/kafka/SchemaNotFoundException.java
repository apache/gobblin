package gobblin.source.extractor.extract.kafka;

@SuppressWarnings("serial")
public class SchemaNotFoundException extends Exception {

  public SchemaNotFoundException(String format) {
    super(format);
  }

}
