* Author: Ziyang
* Reviewer: Chavdar

This page will guide you to set up Gobblin, and run a quick and simple first job.

Download and Build


# Terminology
Term | Definition
---- | ----------
Source	| Represents an external data store we need to pull data from (e.g. Oracle, MySQL, Kafka, Salesforce, etc.)
Extractor | Responsible for pulling a subset of the data from a Source
Watermark | How a job keeps track of its state - keeps track of the offset up to which the job pulled data (e.g. offset, scn, timestamp)
WorkUnit | A collection of key-value pairs required for a Task to execute
WorkUnitState | A collection of key-value pairs that contains all pairs present in WorkUnit, as well as Task runtime key-values pairs (e.g. how many records got written)
Task | This ties all the classes together and is executed in its own thread: it reads data from the extractor, passes it through a series of converters, and then passes it to the writer and data publisher
Entity | A specific topic present in the Source (e.g. Oracle Table or Kafka Topic)
Snapshot | Represents a full dump of an entity
Pull file | Represents all the key-value pairs necessary to run a Gobblin job

# What Can I Use Gobblin For
Gobblin can be used to connect to an external data source and pull data on a periodic basis. The data can be transformed into a variety of output formats (e.g. Avro) and can be written to any storage system (e.g. HDFS). If you have a requirement to pull data from an external store then Gobblin can provide a pluggable, relient, and consistent way of pulling and publishing your data.

# Where to Start
Gobblin can be used to pull data from any external sources. Currently, Gobblin supports a few sources out of the box, and more and more are being added every day. It is common for a user to find that the infrastructure to pull data from their required source is already present. In this case the user can easily add new properties file in they want to pull in a new entity (e.g. table or topic). If Gobblin doesn't support your data source then you only need to add a few Java classes for to everything working.

## Adding a New Data Source
### Extending Base Classes
Gobblin isolates its logic into a few plugin points. The most important of these plugins are the Source class and the Extractor class. The Source class distributes work among a series of Extractors, and each Extractor is responsible for connecting to the external data source and pulling the data into the framework. When pulling in from a new data source the user needs to implement these two interfaces. Once these two classes are implemented the framework can be used to pull data and write it out to a storage system.
#### Source
##### com.linkedin.uif.source.workunit.Extractor
    public interface Source<S, D> {
  
      /**
       * Get a list of {@link WorkUnit}s, each of which is for extracting a portion of the data.
       *
       * <p>
       *   Each {@link WorkUnit} will be used instantiate a {@link WorkUnitState} that gets passed to the
       *   {@link #getExtractor(WorkUnitState)} method to get an {@link Extractor} for extracting schema
       *   and data records from the source. The {@link WorkUnit} instance should have all the properties
       *   needed for the {@link Extractor} to work.
       * </p>
       *
       * <p>
       *   Typically the list of {@link WorkUnit}s for the current run is determined by taking into account
       *   the list of {@link WorkUnit}s from the previous run so data gets extracted incrementally. The
       *   method {@link SourceState#getPreviousWorkUnitStates} can be used to get the list of {@link WorkUnit}s
       *   from the previous run.
       * </p>
       * 
       * @param state see {@link SourceState}
       * @return a list of {@link WorkUnit}s
       */
      public abstract List<WorkUnit> getWorkunits(SourceState state);

      /**
       * Get an {@link Extractor} based on a given {@link WorkUnitState}.
       *
       * <p>
       *   The {@link Extractor} returned can use {@link WorkUnitState} to store arbitrary key-value pairs
       *   that will be persisted to the state store and loaded in the next scheduled job run.
       * </p>
       * 
       * @param state a {@link WorkUnitState} carrying properties needed by the returned {@link Extractor}
       * @return an {@link Extractor} used to extract schema and data records from the data source
       * @throws IOException if it fails to create an {@link Extractor}
       */
      public abstract Extractor<S, D> getExtractor(WorkUnitState state) throws IOException;
  
      /**
       * Shutdown this {@link Source} instance.
       *
       * <p>
       *   This method is called once when the job completes. Properties (key-value pairs) added to the input
       *   {@link SourceState} instance will be persisted and available to the next scheduled job run through
       *   the method {@link #getWorkunits(SourceState)}.  If there is no cleanup or reporting required for a
       *   particular implementation of this interface, then it is acceptable to have a default implementation
       *   of this method.
       * </p>
       * 
       * @param state see {@link SourceState}
       */
      public abstract void shutdown(SourceState state);
    }


The Source class is responsible for splitting the work to be done among a series of WorkUnits. The function getWorkunits should construct a series of WorkUnits and assign a subset of the work to be done to each WorkUnit. The next function is getExtractor which will take in one of the WorkUnits defined in getWorkUnits and construct an Extractor object.

#### Extractor
##### com.linkedin.uif.source.workunit.Extractor
    public interface Extractor<S, D> extends Closeable {
      /**
       * Get the schema (Metadata) of the extracted data records.
       *
       * @return schema of the extracted data records
       */
      public S getSchema();

      /**
       * Read a data record from the data source.
       *
       * <p>
       *   This method allows data record object reuse through the one passed in if the
       *   implementation class decides to do so.
       * </p>
       *
       * @param reuse the data record object to be used
       * @return a data record
       * @throws DataRecordException if there is problem with the extracted data record
       * @throws java.io.IOException if there is problem extract a data record from the source
       */
      public D readRecord(D reuse) throws DataRecordException, IOException;

      /**
       * Get the expected source record count.
       *
       * @return expected source record count
       */
      public long getExpectedRecordCount();

      /**
       * Get the calculated high watermark up to which data records are to be extracted.
       * @return high watermark
       */
      public long getHighWatermark();
    }

The Extractor class is created from a WorkUnit and is responsible for connecting to the external data source, getting the schema for the data that will be pulled, and getting the data. The method readRecord will be called by Gobblin until it returns null, in which case the framework assumes that it has read all the data for that Extractor instance. Thus, the extractor acts as in iterator of a subset of the data to be pulled. The extractor class also requires you to implement two more functions: getExpectedRecordCount and getHighWatermark. The record count method should return the number of expected records that this extractor is going to pull, the get high watermark method should return the high watermark for this extractor (e.g. it should return some value that represents up to what point in the Source this extractor will pull data).

### HelloWorld Extractor and Source
Gobblin has a HelloWorld Extractor and Source that shows a provides a simple implementation of reading and writing Avro files. The full code for the HelloWorldSource class can be found below.
#### com.linkedin.uif.helloworld.source.HelloWorldSource
    public class HelloWorldSource implements Source<String, String> {
        private static final String SOURCE_FILE_LIST_KEY = "source.files";
        private static final String SOURCE_FILE_KEY = "source.file";
        private static final Splitter SPLITTER = Splitter.on(",")
                .omitEmptyStrings()
                .trimResults();

        @Override
        public List<WorkUnit> getWorkunits(SourceState state) {
            Extract extract1 = new Extract(state, TableType.SNAPSHOT_ONLY,
                               state.getProp(ConfigurationKeys.EXTRACT_NAMESPACE_NAME_KEY), "TestTable1");

            Extract extract2 = new Extract(state, TableType.SNAPSHOT_ONLY,
                               state.getProp(ConfigurationKeys.EXTRACT_NAMESPACE_NAME_KEY), "TestTable2");

            String sourceFileList = state.getProp(SOURCE_FILE_LIST_KEY);
            List<WorkUnit> workUnits = Lists.newArrayList();

            List<String> list = SPLITTER.splitToList(sourceFileList);

            for (int i = 0; i < list.size(); i++) {
                WorkUnit workUnit = new WorkUnit(state, i % 2 == 0 ? extract1 : extract2);
                workUnit.setProp(SOURCE_FILE_KEY, list.get(i));
                workUnits.add(workUnit);
            }
            return workUnits;
        }

        @Override
        public Extractor<String, String> getExtractor(WorkUnitState state) {
            return new TestExtractor(state);
        }

        @Override
        public void shutdown(SourceState state) {
            // Do nothing
        }
    }
The Source class creates a list of two WorkUnits for Gobblin to execute and returns them as a list. In order to construct the WorkUnits an Extract object needs to be created. An Extract object represents all the attributes necessary to pull a subset of the data. These properties include:

1. TableType: The type of data being pulled; is it append only data (fact tables), snapshot + append data (dimension tables), or snapshot only data
2. Namespace: A dot separated namespace path
3. Table: Entity name

The Source class is also paired with a pull file (shown below), note that the Source class has access to all key-value pairs created in the pull file. The getExtractor method has a very simple implementation - given a WorkUnitState it creates and returns a TestExtractor object. The code for TestExtractor is shown below.

#### com.linkedin.uif.helloworld.extractor.HelloWorldExtractor
    public class HelloWorldExtractor implements Extractor<String, String> {
        private static final Logger log = LoggerFactory.getLogger(HelloWorldExtractor.class);
        private static final String SOURCE_FILE_KEY = "source.file";

        // Test Avro Schema
        private static final String AVRO_SCHEMA =
                "{\"namespace\": \"example.avro\",\n" +
                " \"type\": \"record\",\n" +
                " \"name\": \"User\",\n" +
                " \"fields\": [\n" +
                "     {\"name\": \"name\", \"type\": \"string\"},\n" +
                "     {\"name\": \"favorite_number\",  \"type\": \"int\"},\n" +
                "     {\"name\": \"favorite_color\", \"type\": \"string\"}\n" +
                " ]\n" +
                "}";

        private static final int TOTAL_RECORDS = 1000;
        private DataFileReader<GenericRecord> dataFileReader;

        public HelloWorldExtractor(WorkUnitState workUnitState) {
            Schema schema = new Schema.Parser().parse(AVRO_SCHEMA);
            Path sourceFile = new Path(workUnitState.getWorkunit().getProp(SOURCE_FILE_KEY));

            log.info("Reading from source file " + sourceFile);
            DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);

            try {
                URI uri = URI.create(workUnitState.getProp(ConfigurationKeys.FS_URI_KEY, ConfigurationKeys.LOCAL_FS_URI));
                FileSystem fs = FileSystem.get(uri, new Configuration());
                fs.makeQualified(sourceFile);
                this.dataFileReader = new DataFileReader<GenericRecord>(
                                      new FsInput(sourceFile,
                                      new Configuration()), datumReader);
            } catch (IOException ioe) {
                log.error("Failed to read the source file " + sourceFile, ioe);
            }
        }

        @Override
        public String getSchema() {
            return AVRO_SCHEMA;
        }

        @Override
        public String readRecord() {
            if (this.dataFileReader == null) {
                return null;
            }
            if (this.dataFileReader.hasNext()) {
                return this.dataFileReader.next().toString();
            }
            return null;
        }

        @Override
        public void close() throws IOException {
            try {
                this.dataFileReader.close();
            } catch (IOException ioe) {
                log.error("Error while closing avro file reader", ioe);
            }
        }

        @Override
        public long getExpectedRecordCount() {
            return TOTAL_RECORDS;
        }

        @Override
        public long getHighWatermark()
        {
          return 0;
        }
    }
This extractor opens up an Avro file and creates a FileReader over the file. The FileReader object implements the Iterator interface, so the readRecord method becomes very simple. It queries the FileReader to see if has more records, if it does it returns the next record, if not it returns null. The schema is defined in line, but it can open up a connection to the Source and fetch the schema for the Entity that is being pulled. The expected record count is also defined in line, but once again the extractor can pull the value from the Source (e.g. SELECT COUNT() FROM EntityName).

A pull file for the HelloWorld classes is shown below.
#### helloworld.pull
    job.name=HelloWorldFilePull
    job.group=HelloWorldJobs
    job.description=Simple job to pull move files
 
    source.class=com.linkedin.uif.helloworld.source.HelloWorldSource
 
    writer.destination.type=HDFS
    writer.output.format=AVRO
    writer.fs.uri=file://localhost/
 
    data.publisher.type=com.linkedin.uif.publisher.BaseDataPublisher
 
    source.files=<Insert location of files to copy>
For the pull file, there are a few required properties necessary for all jobs. A list of config properties and their meanings can be found here: [Configuration Properties](Configuration-Properties)

### Extending Protocol Specific Classes
While any user is free to directly extend the Source and Extractor class, Gobblin also supports Extractors that implement commonly used protocols. These protocols fall into a few major categories (e.g. QueryBasedExtractor, FileBasedExtractor, etc.). Gobblin currently contains implementations of RestApiExtractor and SftpExtractor and the user is free to extend both of these classes in order to take advantage of existing implementations of both the protocols. For example, if a new data source extracts data using a REST service then the RestApiExtractor can easily be extended, which avoids the need to re-implement any REST logic. The layout of the classes is depicted below.
    Extractor.java
        QueryBasedExtractor.java
            RestApiExtractor.java
                SalesforceExtractor.java
            JdbcExtractor.java
                TeradataExtractor.java
        FileBasedExtractor.java
            SftpExtractor.java
                ResponsysExtractor.java

## Leveraging an Existing Source
Once you have your source and extractor class, it is time to create some "pull" files to run the job. A pull file is a list of user specified configuration properties that are fed into the framework. Each WorkUnit has access to each key-value pair in the pull file, this allows the user to pass in Source specific parameters to the framework. An example of a production pull file is below.
    # Job parameters
    job.name=Salesforce_Contact
    job.group=Salesforce_Core
    job.description=Job to pull data from Contact table
    job.schedule=0 0 0/1 * * ?

    # Converter parameters
    converter.classes=com.linkedin.uif.converter.avro.JsonIntermediateToAvroConverter,com.linkedin.uif.converter.LumosAttributesConverter
    converter.avro.timestamp.format=yyyy-MM-dd'T'HH:mm:ss.SSS'Z',yyyy-MM-dd'T'HH:mm:ss.000+0000
    converter.avro.date.format=yyyy-MM-dd
    converter.avro.time.format=HH:mm:ss

    # Writer parameters
    writer.destination.type=HDFS
    writer.output.format=AVRO
    writer.fs.uri=file://localhost/

    # Quality Checker and Publisher parameters
    qualitychecker.task.policies=com.linkedin.uif.policies.count.RowCountPolicy,com.linkedin.uif.policies.schema.SchemaCompatibilityPolicy,com.linkedin.uif.policies.schema.LumosSchemaValidationPolicy
    qualitychecker.task.policy.types=FAIL,OPTIONAL,OPTIONAL
    qualitychecker.row.policies=com.linkedin.uif.policies.schema.SchemaRowCheckPolicy
    qualitychecker.row.policy.types=ERR_FILE
    data.publisher.type=com.linkedin.uif.publisher.BaseDataPublisher

    # Extractor parameters
    extract.namespace=Salesforce_Core
    extract.table.type=snapshot_append
    extract.delta.fields=SystemModstamp
    extract.primary.key.fields=Id

    # Source parameters
    source.schema=Core
    source.entity=Contact
    source.extract.type=snapshot
    source.watermark.type=timestamp
    source.start.value=20140101000000
    source.end.value=201403010000000
    source.low.watermark.backup.secs=7200
    source.is.watermark.override=true
    source.timezone=UTC
    source.max.number.of.partitions=2
    source.partition.interval=2
    source.fetch.size=2000
    source.is.specific.api.active=true
    source.timeout=7200000
    source.class=com.linkedin.uif.source.extractor.extract.restapi.SalesforceSource
A list of all configuration properties and there meanings can be found here: [Configuration Properties](Configuration-Properties)

Extending the Framework
-----------------------
WIP