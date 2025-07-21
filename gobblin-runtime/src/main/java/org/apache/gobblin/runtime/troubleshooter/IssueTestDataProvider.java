package org.apache.gobblin.runtime.troubleshooter;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ErrorCategory;
import org.apache.gobblin.configuration.ErrorPatternProfile;

import static java.util.Collections.singletonMap;

@Slf4j
public class IssueTestDataProvider {

  // Test categories
  public static final List<ErrorCategory> TEST_CATEGORIES = Arrays.asList(
      new ErrorCategory("USER", 1),
      new ErrorCategory("SYSTEM INFRA", 2),
      new ErrorCategory("GAAS", 3),
      new ErrorCategory("MISC", 4),
      new ErrorCategory("NON FATAL", 5),
      new ErrorCategory("UNKNOWN", Integer.MAX_VALUE)
  );

  public static final ErrorCategory TEST_DEFAULT_ERROR_CATEGORY = new ErrorCategory("UNKNOWN", Integer.MAX_VALUE);

  // Test patterns - you'll need to add all the patterns here
  public static final List<ErrorPatternProfile> TEST_PATTERNS = Arrays.asList(
        new ErrorPatternProfile(".*the namespace quota.*", "USER"),
        new ErrorPatternProfile(".*permission denied.*", "USER"),
        new ErrorPatternProfile(".*remoteexception.*read only mount point.*", "USER"),
        new ErrorPatternProfile(".*remoteexception: operation category write is not supported in state observer.*", "USER"),
        new ErrorPatternProfile(".*invalidoperationexception: alter is not possible.*", "USER"),
        new ErrorPatternProfile(".*the diskspace quota.*", "USER"),
        new ErrorPatternProfile(".*filenotfoundexception:.*", "USER"),
        new ErrorPatternProfile(".*does not exist.*", "USER"),
        new ErrorPatternProfile("remoteexception: file/directory.*does not exist", "USER"),
        new ErrorPatternProfile("runtimeexception: directory not found.*", "USER"),
        new ErrorPatternProfile(".*icebergtable.tablenotfoundexception.*", "USER"),
        new ErrorPatternProfile("ioexception: topath.*must be an ancestor of frompath.*", "USER"),
        new ErrorPatternProfile("ioexception: origin path.*does not exist.*", "USER"),
        new ErrorPatternProfile("illegalargumentexception: missing source iceberg table.*", "USER"),
        new ErrorPatternProfile("remoteexception:.*already exists \\| failed to send re-scaling directive.*", "USER"),
        new ErrorPatternProfile("illegalargumentexception: when replacing prefix, all locations must be descendants of the prefix.*", "GAAS"),
        new ErrorPatternProfile("illegalargumentexception:.*", "MISC"),
        new ErrorPatternProfile("wrongargumentexception: '�' is not a valid numeric or approximate numeric value \\| failed to commit writer for partition.*", "USER"),
        new ErrorPatternProfile(".*remoteexception: server too busy.*", "SYSTEM INFRA"),
        new ErrorPatternProfile("jschexception: session is down.*", "SYSTEM INFRA"),
        new ErrorPatternProfile("jschexception: channel request.*", "SYSTEM INFRA"),
        new ErrorPatternProfile(".*safemodeexception.*", "SYSTEM INFRA"),
        new ErrorPatternProfile("remoteexception: no namenode available to invoke.*", "SYSTEM INFRA"),
        new ErrorPatternProfile("remoteexception: org.apache.hadoop.hdfs.server.federation.router.nonamenodesavailableexception: no namenodes available under nameservice.*", "SYSTEM INFRA"),
        new ErrorPatternProfile(".*no namenodes available under nameservice.*", "SYSTEM INFRA"),
        new ErrorPatternProfile(".*sockettimeoutexception.*", "SYSTEM INFRA"),
        new ErrorPatternProfile(".*socketexception: connection reset.*", "SYSTEM INFRA"),
        new ErrorPatternProfile("timeoutexception: failed to update metadata after 60000 ms.*", "SYSTEM INFRA"),
        new ErrorPatternProfile("networkexception: the server disconnected before a response was received.*", "SYSTEM INFRA"),
        new ErrorPatternProfile("webclientrequestwithmessageexception: handshake timed out after 10000ms.*", "SYSTEM INFRA"),
        new ErrorPatternProfile("timeoutfailure: message='activity heartbeat timeout.*", "GAAS"),
        new ErrorPatternProfile("timeoutexception: failed to allocate memory within the configured max blocking time.*", "GAAS"),
        new ErrorPatternProfile("timeoutfailure: message='activity starttoclose timeout'.*", "GAAS"),
        new ErrorPatternProfile("timeoutexception: topic gobblintrackingevent_sgs not present in metadata after 60000 ms.*", "GAAS"),
        new ErrorPatternProfile("connectexception: connection refused.*", "SYSTEM INFRA"),
        new ErrorPatternProfile("connectionisclosedexception: no operations allowed after connection closed.*", "SYSTEM INFRA"),
        new ErrorPatternProfile("ioexception: failed to get connection for.*is already stopped.*", "SYSTEM INFRA"),
        new ErrorPatternProfile("ioexception: unable to close file because dfsclient was unable to contact the hdfs servers.*", "SYSTEM INFRA"),
        new ErrorPatternProfile(".*hikaripool-.*-datasourceprovider - failed to execute connection test query.*", "SYSTEM INFRA"),
        new ErrorPatternProfile(".*interruptedioexception.*", "MISC"),
        new ErrorPatternProfile("interruptedexception: sleep interrupted.*", "MISC"),
        new ErrorPatternProfile("closedbyinterruptexception.*", "MISC"),
        new ErrorPatternProfile("interruptedexception:  \\| failed to commit writer for partition.*", "MISC"),
        new ErrorPatternProfile(".*http connection failed for getting token from azuread.*", "USER"),
        new ErrorPatternProfile("ioexception: invalid token.*", "USER"),
        new ErrorPatternProfile(".*remoteexception: token.*can't be found in cache.*", "SYSTEM INFRA"),
        new ErrorPatternProfile(".*applicationfailure:.*token.*can't be found in cache.*", "SYSTEM INFRA"),
        new ErrorPatternProfile(".*closedchannelexception.*", "MISC"),
        new ErrorPatternProfile("applicationfailure: message='task failed: org.apache.gobblin.runtime.forkexception.*", "MISC"),
        new ErrorPatternProfile(".*eofexception.*", "MISC"),
        new ErrorPatternProfile("canceledfailure: child canceled.*", "MISC"),
        new ErrorPatternProfile(".*message='unable to create new native thread'.*", "GAAS"),
        new ErrorPatternProfile("ioexception: failed to register hive spec simplehivespec.*", "USER"),
        new ErrorPatternProfile("ioexception: error: likely concurrent writing to destination: \\(just prior\\) tablemetadata.*", "MISC"),
        new ErrorPatternProfile(".*nullpointerexception:.*", "MISC"),
        new ErrorPatternProfile(".*classnotfoundexception.*", "MISC"),
        new ErrorPatternProfile(".*noclassdeffounderror: could not initialize class.*", "MISC"),
        new ErrorPatternProfile(".*failure in getting work units for job.*", "MISC"),
        new ErrorPatternProfile(".*operation failed: \"the uploaded data is not contiguous or the position query parameter value is not equal to the length of the file after appending the uploaded data.*", "MISC"),
        new ErrorPatternProfile(".*applicationfailure: message='task failed: operation failed: \"the uploaded data is not contiguous or the position query parameter value is not equal to the length of the file after appending the uploaded data.*", "MISC"),
        new ErrorPatternProfile(".*could not create work unit to deregister partition.*", "MISC"),
        new ErrorPatternProfile("runtimeexception: not committing dataset.*", "MISC"),
        new ErrorPatternProfile("job.*has unfinished commit sequences. will not clean up staging data", "MISC"),
        new ErrorPatternProfile(".*failed to commit.*dataset.*", "MISC"),
        new ErrorPatternProfile(".*could not commit file.*", "MISC"),
        new ErrorPatternProfile("runtimeexception: failed to parse the date.*", "USER"),
        new ErrorPatternProfile("runtimeexception: clean failed for one or more datasets.*", "MISC"),
        new ErrorPatternProfile("runtimeexception: not copying.*", "MISC"),
        new ErrorPatternProfile(".*failed to read from all available datanodes.*", "NON FATAL"),
        new ErrorPatternProfile(".*failed to prepare extractor: error - failed to get schema for this object.*", "NON FATAL"),
        new ErrorPatternProfile(".*failed to get primary group for kafkaetl, using user name as primary group name.*", "NON FATAL"),
        new ErrorPatternProfile(".*failed to delete.*", "NON FATAL"),
        new ErrorPatternProfile(".*applicationfailure: message='task failed: java.lang.arrayindexoutofboundsexception.*", "NON FATAL"),
        new ErrorPatternProfile(".*arrayindexoutofboundsexception.*failed to close all open resources", "NON FATAL"),
        new ErrorPatternProfile("ioexception: failed to clean up all writers. \\| failed to close all open resources.*", "MISC"),
        new ErrorPatternProfile("applicationfailure:.*failed to clean up all writers.*", "MISC"),
        new ErrorPatternProfile("applicationfailure: message='task failed: java.io.ioexception: failed to commit all writers..*", "MISC"),
        new ErrorPatternProfile(".*metaexception.*", "USER"),
        new ErrorPatternProfile(".*ioexception: filesystem closed.*", "GAAS"),
        new ErrorPatternProfile(".*source and target table are not compatible.*", "USER"),
        new ErrorPatternProfile("hivetablelocationnotmatchexception: desired target location.*do not agree.*", "USER"),
        new ErrorPatternProfile(".*schema mismatch between metadata.*", "USER"),
        new ErrorPatternProfile(".*could not read all task state files.*", "MISC"),
        new ErrorPatternProfile("taskstatestore successfully opened, but no task states found under.*", "MISC"),
        new ErrorPatternProfile(".*setting task state to failed.*", "MISC"),
        new ErrorPatternProfile("illegalstateexception: expected end_array.*", "GAAS"),
        new ErrorPatternProfile("illegalstateexception: cannot perform operation after producer has been closed.*", "GAAS"),
        new ErrorPatternProfile("webclientresponse.*503 service unavailable.*", "SYSTEM INFRA"),
        new ErrorPatternProfile("webclientresponseexception.*504.*", "SYSTEM INFRA"),
        new ErrorPatternProfile("webclientresponsewithmessageexception.*504.*", "SYSTEM INFRA"),
        new ErrorPatternProfile("webclientresponseexception.*400.*", "USER"),
        new ErrorPatternProfile("webclientresponseexception.*409.*", "USER"),
        new ErrorPatternProfile("webclientresponsewithmessageexception.*402.*", "USER"),
        new ErrorPatternProfile(".*fsstatestore.*", "MISC"),
        new ErrorPatternProfile("sqltransientconnectionexception:.*connection is not available, request timed out after.*", "SYSTEM INFRA"),
        new ErrorPatternProfile("sqlnontransientconnectionexception: got timeout reading communication packets.*", "SYSTEM INFRA"),
        new ErrorPatternProfile("sqlexception: unsupported transaction isolation level '-1'.*", "SYSTEM INFRA"),
        new ErrorPatternProfile("sqlexception: incorrect string value:.*for.*", "USER"),
        new ErrorPatternProfile(".*obstateutils.opentaskstatestoreuncached debug.*", "MISC"),
        new ErrorPatternProfile("[bug] workflow thread.*can't be destroyed in time. this will lead to a workflow cache leak. this problem is usually caused by a workflow implementation swallowing java.lang.error instead of rethrowing it.*", "SYSTEM INFRA"),
        new ErrorPatternProfile(".*outofmemoryerror.*", "GAAS"),
        new ErrorPatternProfile("filealreadyexistsexception: rename destination.*", "NON FATAL"),
        new ErrorPatternProfile("applicationfailure: message='failed to rename.*", "MISC"),
        new ErrorPatternProfile("potentialdeadlockexception: potential deadlock detected.*", "GAAS"),
        new ErrorPatternProfile("workflow lock for the run id hasn't been released by one of previous execution attempts, consider increasing workflow task timeout.*", "GAAS"),
        new ErrorPatternProfile("nosuchelementexception:  \\| failed to commit writer for partition.*", "MISC"),
        new ErrorPatternProfile("illegalargumentexception: cannot instantiate jdbcpublisher since it does not extend singletaskdatapublisher.*", "MISC"),
        new ErrorPatternProfile("processing failure:.*retrystate=retry_state.*commit workflow failure", "MISC"),
        new ErrorPatternProfile(".*recordtoolargeexception.*", "MISC"),
        new ErrorPatternProfile("ioexception: could not get block locations. source file", "MISC"),
        new ErrorPatternProfile("ioexception: job status not available  \\| failed to launch and run job.*", "MISC"),
        new ErrorPatternProfile("enqueuing of event.*timed out. sending of events is probably stuck", "MISC"),
        new ErrorPatternProfile("applicationfailure: message='task failed: status code: -1 error code.*", "MISC"),
        new ErrorPatternProfile("applicationfailure: message='failing in submitting at least one task before execution.'.*", "MISC"),
        new ErrorPatternProfile(".*watermark type simple not recognized.*", "MISC")
  );

  public static List<ErrorPatternProfile> getSortedPatterns() {
    Map<String, Integer> categoryPriority = new HashMap<>();
    for (ErrorCategory errorCategory : TEST_CATEGORIES) {
      categoryPriority.put(errorCategory.getCategoryName(), errorCategory.getPriority());
    }

    List<ErrorPatternProfile> sortedPatterns = new ArrayList<>(TEST_PATTERNS);
    sortedPatterns.sort(Comparator.comparingInt(e -> categoryPriority.getOrDefault(e.getCategoryName(), Integer.MAX_VALUE)));
    return sortedPatterns;
  }

  public static List<Issue> testUserCategoryIssues() {
    log.info("Generating test data for USER category issues");
    List<Issue> issues = new ArrayList<>();

    // Namespace Quota issues
    issues.add(new Issue(ZonedDateTime.now(), IssueSeverity.ERROR, "ABCD123",
        "Job failed due to the namespace quota being exceeded for user data", "ContextA", "namespaceQuotaError",
        "quotaexception", singletonMap("keyA", "valueA")));

    // Permission Issues
    issues.add(new Issue(ZonedDateTime.now().minusDays(1), IssueSeverity.ERROR, "ABCD123",
        "Task failed with permission denied when accessing secure directory", "ContextB", "permissionError",
        "securityexception", singletonMap("keyB", "valueB")));
    issues.add(new Issue(ZonedDateTime.now().minusHours(2), IssueSeverity.ERROR, "ABCD123",
        "RemoteException: operation category write is not supported in state observer mode", "ContextC",
        "writePermissionError", "remoteexception", singletonMap("keyC", "valueC")));

    // Diskspace Quota Issues
    issues.add(new Issue(ZonedDateTime.now().minusHours(3), IssueSeverity.ERROR, "ABCD123",
        "Operation failed because the diskspace quota has been reached", "ContextD", "diskQuotaError", "quotaexception",
        singletonMap("keyD", "valueD")));

    // File Not Found Issues
    issues.add(new Issue(ZonedDateTime.now().minusHours(4), IssueSeverity.ERROR, "ABCD123",
        "FileNotFoundException: /path/to/missing/file.txt not found", "ContextE", "fileNotFoundError",
        "filenotfoundexception", singletonMap("keyE", "valueE")));
    issues.add(new Issue(ZonedDateTime.now().minusHours(5), IssueSeverity.ERROR, "ABCD123",
        "RemoteException: file/directory /data/temp does not exist", "ContextF", "directoryNotFoundError",
        "remoteexception", singletonMap("keyF", "valueF")));

    // Token Issues
    issues.add(new Issue(ZonedDateTime.now().minusHours(6), IssueSeverity.ERROR, "ABCD123",
        "HTTP connection failed for getting token from AzureAD authentication service", "ContextG", "tokenError",
        "authexception", singletonMap("keyG", "valueG")));

    return issues;
  }

  public static List<Issue> testSystemInfraCategoryIssues() {
    // Returns a list of Issues that should match SYSTEM INFRA category regex patterns
    log.info("Generating test data for SYSTEM_INFRA category issues");
    List<Issue> issues = new ArrayList<>();

    // Server Issues
    issues.add(new Issue(ZonedDateTime.now(), IssueSeverity.ERROR, "SYSTEM_INFRA",
        "RemoteException: server too busy to handle request at this time", "ContextA", "serverBusyError",
        "remoteexception", singletonMap("keyA", "valueA")));
    issues.add(new Issue(ZonedDateTime.now().minusDays(1), IssueSeverity.ERROR, "SYSTEM_INFRA",
        "JSchException: session is down and cannot be restored", "ContextB", "sessionDownError", "jschexception",
        singletonMap("keyB", "valueB")));

    // Namenode Issues
    issues.add(new Issue(ZonedDateTime.now().minusHours(2), IssueSeverity.ERROR, "SYSTEM_INFRA",
        "SafeModeException: Name node is in safe mode and cannot accept changes", "ContextC", "safeModeError",
        "safemodeexception", singletonMap("keyC", "valueC")));
    issues.add(new Issue(ZonedDateTime.now().minusHours(3), IssueSeverity.ERROR, "SYSTEM_INFRA",
        "RemoteException: no namenode available to invoke for cluster operations", "ContextD", "namenodeError",
        "remoteexception", singletonMap("keyD", "valueD")));

    // Timeout Issues
    issues.add(new Issue(ZonedDateTime.now().minusHours(4), IssueSeverity.ERROR, "SYSTEM_INFRA",
        "SocketTimeoutException: Read timeout after 30000ms waiting for response", "ContextE", "timeoutError",
        "sockettimeoutexception", singletonMap("keyE", "valueE")));
    issues.add(new Issue(ZonedDateTime.now().minusHours(5), IssueSeverity.ERROR, "SYSTEM_INFRA",
        "SocketException: connection reset by peer during data transfer", "ContextF", "connectionResetError",
        "socketexception", singletonMap("keyF", "valueF")));

    // Connection Issues
    issues.add(new Issue(ZonedDateTime.now().minusHours(6), IssueSeverity.ERROR, "SYSTEM_INFRA",
        "ConnectException: connection refused when trying to connect to remote service", "ContextG",
        "connectionRefusedError", "connectexception", singletonMap("keyG", "valueG")));

    return issues;
  }

  public static List<Issue> testGaasCategoryIssues() {
    // Returns a list of Issues that should match GAAS category regex patterns
    log.info("Generating test data for GAAS category issues");
    List<Issue> issues = new ArrayList<>();

    String testString = "TimeoutFailure: message='activity heartbeat timeout exceeded configured limit'"; // TBD: DELETE
    // Timeout Issues
    issues.add(
        new Issue(ZonedDateTime.now(), IssueSeverity.ERROR, "GAAS", testString, "ContextA", "activityTimeoutError",
            "timeoutfailure", singletonMap("keyA", "valueA")));

    issues.add(new Issue(ZonedDateTime.now().minusDays(1), IssueSeverity.ERROR, "GAAS",
        "TimeoutException: failed to allocate memory within the configured max blocking time of 5000ms", "ContextB",
        "memoryTimeoutError", "timeoutexception", singletonMap("keyB", "valueB")));

    // Thread Issues
    issues.add(new Issue(ZonedDateTime.now().minusHours(2), IssueSeverity.ERROR, "GAAS",
        "ApplicationFailure: message='unable to create new native thread - system limits reached'", "ContextC",
        "threadCreationError", "applicationfailure", singletonMap("keyC", "valueC")));

    // Filesystem Issues
    issues.add(new Issue(ZonedDateTime.now().minusHours(3), IssueSeverity.ERROR, "GAAS",
        "IOException: filesystem closed unexpectedly during operation", "ContextD", "filesystemClosedError",
        "ioexception", singletonMap("keyD", "valueD")));

    // Illegal State Issues
    issues.add(new Issue(ZonedDateTime.now().minusHours(4), IssueSeverity.ERROR, "GAAS",
        "IllegalStateException: expected END_ARRAY but found different token type", "ContextE", "illegalStateError",
        "illegalstateexception", singletonMap("keyE", "valueE")));

    return issues;
  }

  public static List<Issue> testMiscCategoryIssues() {
    // Returns a list of Issues that should match MISC category regex patterns
    log.info("Generating test data for MISC category issues");
    List<Issue> issues = new ArrayList<>();

    // Interruption Issues
    issues.add(new Issue(ZonedDateTime.now(), IssueSeverity.WARN, "MISC",
        "InterruptedIOException occurred during file operation", "ContextA", "interruptedIOError",
        "interruptedioexception", singletonMap("keyA", "valueA")));
    issues.add(new Issue(ZonedDateTime.now().minusDays(1), IssueSeverity.WARN, "MISC",
        "InterruptedException: sleep interrupted by external signal", "ContextB", "sleepInterruptedError",
        "interruptedexception", singletonMap("keyB", "valueB")));

    // Null Pointer Issues
    issues.add(new Issue(ZonedDateTime.now().minusHours(2), IssueSeverity.ERROR, "MISC",
        "NullPointerException: attempt to invoke method on null object reference", "ContextC", "nullPointerError",
        "nullpointerexception", singletonMap("keyC", "valueC")));

    // Class Not Found Issues
    issues.add(new Issue(ZonedDateTime.now().minusHours(3), IssueSeverity.ERROR, "MISC",
        "ClassNotFoundException: com.example.MissingClass not found in classpath", "ContextD", "classNotFoundError",
        "classnotfoundexception", singletonMap("keyD", "valueD")));

    // Concurrent Write Issues
    issues.add(new Issue(ZonedDateTime.now().minusHours(4), IssueSeverity.ERROR, "MISC",
        "IOException: error: likely concurrent writing to destination: (just prior) TableMetadata collision detected",
        "ContextE", "concurrentWriteError", "ioexception", singletonMap("keyE", "valueE")));

    return issues;
  }

  public static List<Issue> testNonFatalCategoryIssues() {
    log.info("Generating test data for NON_FATAL category issues");
    // Returns a list of Issues that should match NON FATAL category regex patterns
    List<Issue> issues = new ArrayList<>();

    // Copy Data Publisher Issues
    issues.add(new Issue(ZonedDateTime.now(), IssueSeverity.WARN, "NON_FATAL",
        "CopyDataPublisher warning: srcfs operation completed with minor issues", "ContextA", "copyDataWarning",
        "copydatapublisher", singletonMap("keyA", "valueA")));

    // Failed Operations (Non-Fatal)
    issues.add(new Issue(ZonedDateTime.now().minusDays(1), IssueSeverity.WARN, "NON_FATAL",
        "Warning: failed to read from all available datanodes, retrying with backup nodes", "ContextB",
        "datanodeReadWarning", "ioexception", singletonMap("keyB", "valueB")));
    issues.add(new Issue(ZonedDateTime.now().minusHours(2), IssueSeverity.INFO, "NON_FATAL",
        "Info: failed to delete temporary file - cleanup will retry later", "ContextC", "deleteFailureInfo",
        "ioexception", singletonMap("keyC", "valueC")));

    // General Non-fatal Issues
    issues.add(new Issue(ZonedDateTime.now().minusHours(3), IssueSeverity.WARN, "NON_FATAL",
        "Exception thrown from InGraphsReporter#report. Exception was suppressed and processing continued", "ContextD",
        "reporterException", "reporterexception", singletonMap("keyD", "valueD")));

    return issues;
  }

  public static List<Issue> testNegativeMatchCases() {
    log.info("Generating test data for negative match cases");
    // Returns a list of Issues that should NOT match any specific regex patterns
    List<Issue> issues = new ArrayList<>();

    // Generic success messages
    issues.add(
        new Issue(ZonedDateTime.now(), IssueSeverity.INFO, "SUCCESS", "Job completed successfully without any errors",
            "ContextA", "successInfo", "success", singletonMap("keyA", "valueA")));

    // Custom application errors not in regex patterns
    issues.add(new Issue(ZonedDateTime.now().minusDays(1), IssueSeverity.ERROR, "CUSTOM",
        "CustomBusinessLogicException: validation failed for user input", "ContextB", "customError",
        "businessexception", singletonMap("keyB", "valueB")));

    // Generic warnings that don't match patterns
    issues.add(new Issue(ZonedDateTime.now().minusHours(2), IssueSeverity.WARN, "GENERIC",
        "Process completed with warnings but no critical errors detected", "ContextC", "genericWarning", "warning",
        singletonMap("keyC", "valueC")));

    // Unrelated error messages
    issues.add(new Issue(ZonedDateTime.now().minusHours(3), IssueSeverity.ERROR, "UNRELATED",
        "Database connection pool exhausted - increase pool size configuration", "ContextD", "poolError",
        "poolexception", singletonMap("keyD", "valueD")));

    return issues;
  }

  public static List<Issue> testEdgeCasesAndBoundaryConditions() {
    log.info("Generating test data for edge cases and boundary conditions");
    // Returns a list of Issues to test edge cases and boundary conditions
    List<Issue> issues = new ArrayList<>();

    // Case sensitivity tests
    issues.add(new Issue(ZonedDateTime.now(), IssueSeverity.ERROR, "CASE_TEST",
        "FILENOTFOUNDEXCEPTION: uppercase version of file not found error", "ContextA", "uppercaseError",
        "filenotfoundexception", singletonMap("keyA", "valueA")));

    // Partial pattern matches
    issues.add(new Issue(ZonedDateTime.now().minusDays(1), IssueSeverity.ERROR, "PARTIAL_TEST",
        "This is a permission error but not exactly permission denied", "ContextB", "partialPermissionError",
        "securityexception", singletonMap("keyB", "valueB")));

    // Multiple pattern matches in single description
    issues.add(new Issue(ZonedDateTime.now().minusHours(2), IssueSeverity.ERROR, "MULTI_MATCH",
        "FileNotFoundException: permission denied when accessing the namespace quota restricted file", "ContextC",
        "multiMatchError", "complexexception", singletonMap("keyC", "valueC")));

    // Empty and null-like descriptions
    issues.add(new Issue(ZonedDateTime.now().minusHours(3), IssueSeverity.ERROR, "EMPTY_TEST", "", "ContextD",
        "emptyDescriptionError", "unknownexception", singletonMap("keyD", "valueD")));
    issues.add(new Issue(ZonedDateTime.now().minusHours(4), IssueSeverity.ERROR, "NULL_TEST", null, "ContextE",
        "nullDescriptionError", "unknownexception", singletonMap("keyE", "valueE")));

    // Very long descriptions
    issues.add(new Issue(ZonedDateTime.now().minusHours(5), IssueSeverity.ERROR, "LONG_TEST",
        "This is a very long error description that contains the phrase 'permission denied' somewhere in the middle of a lot of other text that might make pattern matching more challenging to ensure our regex patterns work correctly even with verbose error messages",
        "ContextF", "longDescriptionError", "verboseexception", singletonMap("keyF", "valueF")));

    return issues;
  }

  public static List<Issue> testNonFatalAndUnknownMix() {
    // Returns a list of Issues that should match NON FATAL and UNKNOWN categories
    List<Issue> issues = new ArrayList<>();

    // NON FATAL issues
    issues.add(new Issue(ZonedDateTime.now(), IssueSeverity.WARN, "NON_FATAL",
        "CopyDataPublisher warning: srcfs operation completed with minor issues", "ContextA", "copyDataWarning",
        "copydatapublisher", singletonMap("keyA", "valueA")));
    issues.add(new Issue(ZonedDateTime.now().minusHours(1), IssueSeverity.INFO, "NON_FATAL",
        "Info: failed to delete temporary file - cleanup will retry later", "ContextB", "deleteFailureInfo",
        "ioexception", singletonMap("keyB", "valueB")));

    // UNKNOWN issues (should not match any known category)
    issues.add(new Issue(ZonedDateTime.now().minusHours(2), IssueSeverity.ERROR, "UNKNOWN",
        "CustomApplicationException: unexpected error occurred in module", "ContextC", "customError", "customexception",
        singletonMap("keyC", "valueC")));
    issues.add(new Issue(ZonedDateTime.now().minusHours(3), IssueSeverity.WARN, "UNKNOWN",
        "Process completed with warnings but no critical errors detected", "ContextD", "genericWarning", "warning",
        singletonMap("keyD", "valueD")));

    return issues;
  }

  public static List<Issue> testMaximumErrorCountExceeded() {
    // Returns a list of Issues where the number of errors exceeds the maximum allowed count
    // This should trigger overflow/limit logic to handle excessive error volumes
    List<Issue> issues = new ArrayList<>();

    // Add 15 USER errors (highest priority)
    for (int i = 0; i < 15; i++) {
      issues.add(new Issue(ZonedDateTime.now().minusMinutes(i), IssueSeverity.ERROR, "USER",
          "FileNotFoundException: /path/to/missing/file" + i + ".txt not found", "ContextU" + i,
          "fileNotFoundError" + i, "filenotfoundexception", singletonMap("keyU" + i, "valueU" + i)));
    }

    // Add 15 SYSTEM INFRA errors
    for (int i = 0; i < 15; i++) {
      issues.add(new Issue(ZonedDateTime.now().minusMinutes(15 + i), IssueSeverity.ERROR, "SYSTEM_INFRA",
          "RemoteException: server too busy to handle request " + i, "ContextS" + i, "serverBusyError" + i,
          "remoteexception", singletonMap("keyS" + i, "valueS" + i)));
    }

    // Add 15 GAAS errors
    for (int i = 0; i < 15; i++) {
      issues.add(new Issue(ZonedDateTime.now().minusMinutes(30 + i), IssueSeverity.ERROR, "GAAS",
          "TimeoutFailure: message='activity heartbeat timeout " + i + "'", "ContextG" + i, "timeoutError" + i,
          "timeoutfailure", singletonMap("keyG" + i, "valueG" + i)));
    }

    // Add 15 MISC errors
    for (int i = 0; i < 15; i++) {
      issues.add(new Issue(ZonedDateTime.now().minusMinutes(45 + i), IssueSeverity.ERROR, "MISC",
          "NullPointerException: null reference in operation " + i, "ContextM" + i, "nullPointerError" + i,
          "nullpointerexception", singletonMap("keyM" + i, "valueM" + i)));
    }

    // Add 10 NON FATAL errors (lower priority)
    for (int i = 0; i < 10; i++) {
      issues.add(new Issue(ZonedDateTime.now().minusMinutes(60 + i), IssueSeverity.WARN, "NON_FATAL",
          "Warning: failed to delete temporary file " + i + " - cleanup will retry later", "ContextN" + i,
          "deleteFailureWarning" + i, "ioexception", singletonMap("keyN" + i, "valueN" + i)));
    }

    return issues;
  }

  public static List<Issue> testMinimumErrorCountBelowThreshold() {
    // Returns a list of Issues where the number of errors is less than the minimum required for a category
    // This should not trigger category match or should be handled gracefully
    List<Issue> issues = new ArrayList<>();

    // Single error from each category - assuming minimum threshold is > 1
    // These should either not be classified or handled with special logic

    // Single USER error
    issues.add(new Issue(ZonedDateTime.now(), IssueSeverity.ERROR, "USER",
        "FileNotFoundException: /single/missing/file.txt not found", "ContextU1", "singleFileNotFoundError",
        "filenotfoundexception", singletonMap("keyU1", "valueU1")));

    // Single SYSTEM INFRA error
    issues.add(new Issue(ZonedDateTime.now().minusMinutes(5), IssueSeverity.ERROR, "SYSTEM_INFRA",
        "RemoteException: server too busy - single occurrence", "ContextS1", "singleServerBusyError", "remoteexception",
        singletonMap("keyS1", "valueS1")));

    // Single GAAS error
    issues.add(new Issue(ZonedDateTime.now().minusMinutes(10), IssueSeverity.ERROR, "GAAS",
        "TimeoutFailure: message='single activity heartbeat timeout'", "ContextG1", "singleTimeoutError",
        "timeoutfailure", singletonMap("keyG1", "valueG1")));

    // Single MISC error
    issues.add(new Issue(ZonedDateTime.now().minusMinutes(15), IssueSeverity.ERROR, "MISC",
        "NullPointerException: single null reference occurrence", "ContextM1", "singleNullPointerError",
        "nullpointerexception", singletonMap("keyM1", "valueM1")));

    // Single NON FATAL error
    issues.add(new Issue(ZonedDateTime.now().minusMinutes(20), IssueSeverity.WARN, "NON_FATAL",
        "Warning: single failed delete operation - cleanup will retry", "ContextN1", "singleDeleteFailureWarning",
        "ioexception", singletonMap("keyN1", "valueN1")));

    return issues;
  }

  public static List<Issue> testOverflowLargeNumberOfErrors() {
    // Returns a list of Issues exceeding the display/processing limit, ordered from lowest to highest priority
    List<Issue> issues = new ArrayList<>();

    // Add 5 UNKNOWN (lowest priority) - errors that don't match any regex patterns
    String[] unknownErrors =
        {"CustomBusinessLogicException: validation failed for user input", "DatabaseConnectionException: connection pool exhausted", "ConfigurationException: invalid property value detected", "SecurityException: authentication failed with external service", "NetworkException: unable to reach external API endpoint"};
    for (int i = 0; i < 5; i++) {
      issues.add(new Issue(ZonedDateTime.now().minusMinutes(60 + i), IssueSeverity.INFO, "UNKNOWN", unknownErrors[i],
          "ContextU" + i, "unknownError" + i, "unknownexception", singletonMap("keyU" + i, "valueU" + i)));
    }

    // Add 5 NON FATAL - errors that match NON FATAL regex patterns
    String[] nonFatalErrors =
        {"CopyDataPublisher warning: srcfs operation completed with minor issues", "Warning: failed to read from all available datanodes, retrying with backup nodes", "Info: failed to delete temporary file - cleanup will retry later", "Exception thrown from InGraphsReporter#report. Exception was suppressed and processing continued", "ApplicationFailure: message='task failed: java.lang.ArrayIndexOutOfBoundsException at index 5'"};
    for (int i = 0; i < 5; i++) {
      issues.add(new Issue(ZonedDateTime.now().minusMinutes(50 + i), IssueSeverity.WARN, "NON_FATAL", nonFatalErrors[i],
          "ContextN" + i, "nonFatalError" + i, "nonfatalexception", singletonMap("keyN" + i, "valueN" + i)));
    }

    // Add 5 MISC - errors that match MISC regex patterns
    String[] miscErrors =
        {"InterruptedIOException occurred during file operation", "NullPointerException: attempt to invoke method on null object reference", "ClassNotFoundException: com.example.MissingClass not found in classpath", "IOException: error: likely concurrent writing to destination: (just prior) TableMetadata collision detected", "ApplicationFailure: failed to clean up all writers during shutdown"};
    for (int i = 0; i < 5; i++) {
      issues.add(new Issue(ZonedDateTime.now().minusMinutes(40 + i), IssueSeverity.ERROR, "MISC", miscErrors[i],
          "ContextM" + i, "miscError" + i, "miscexception", singletonMap("keyM" + i, "valueM" + i)));
    }

    // Add 5 GAAS - errors that match GAAS regex patterns
    String[] gaasErrors =
        {"TimeoutFailure: message='activity heartbeat timeout exceeded configured limit'", "TimeoutException: failed to allocate memory within the configured max blocking time of 5000ms", "ApplicationFailure: message='unable to create new native thread - system limits reached'", "IOException: filesystem closed unexpectedly during operation", "IllegalStateException: expected END_ARRAY but found different token type"};
    for (int i = 0; i < 5; i++) {
      issues.add(new Issue(ZonedDateTime.now().minusMinutes(30 + i), IssueSeverity.ERROR, "GAAS", gaasErrors[i],
          "ContextG" + i, "gaasError" + i, "gaasexception", singletonMap("keyG" + i, "valueG" + i)));
    }

    // Add 5 SYSTEM INFRA - errors that match SYSTEM INFRA regex patterns
    String[] systemInfraErrors =
        {"RemoteException: server too busy to handle request at this time", "JSchException: session is down and cannot be restored", "SafeModeException: Name node is in safe mode and cannot accept changes", "SocketTimeoutException: Read timeout after 30000ms waiting for response", "ConnectException: connection refused when trying to connect to remote service"};
    for (int i = 0; i < 5; i++) {
      issues.add(
          new Issue(ZonedDateTime.now().minusMinutes(20 + i), IssueSeverity.ERROR, "SYSTEM_INFRA", systemInfraErrors[i],
              "ContextS" + i, "systemInfraError" + i, "systeminfraexception", singletonMap("keyS" + i, "valueS" + i)));
    }

    // Add 5 USER (highest priority) - errors that match USER regex patterns
    String[] userErrors =
        {"Job failed due to the namespace quota being exceeded for user data", "Task failed with permission denied when accessing secure directory", "Operation failed because the diskspace quota has been reached", "FileNotFoundException: /path/to/missing/file.txt not found", "HTTP connection failed for getting token from AzureAD authentication service"};
    for (int i = 0; i < 5; i++) {
      issues.add(new Issue(ZonedDateTime.now().minusMinutes(10 + i), IssueSeverity.ERROR, "USER", userErrors[i],
          "ContextUR" + i, "userError" + i, "userexception", singletonMap("keyUR" + i, "valueUR" + i)));
    }

    return issues;
  }
}

