package gobblin.ingestion.google.adwords;

import java.rmi.RemoteException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.ads.adwords.axis.v201609.mcm.ManagedCustomer;
import com.google.api.ads.adwords.lib.client.AdWordsSession;
import com.google.api.ads.common.lib.exception.ValidationException;
import com.google.common.base.Joiner;
import com.google.common.collect.Sets;


public class GoogleAdWordsExtractor {

  private final static Logger LOG = LoggerFactory.getLogger(GoogleAdWordsExtractor.class);

  private final String _masterCustomerId;
  private Properties _configurations;
  private final GoogleAdWordsCredential _credential;
  private final Set<String> _exactAccounts;
  private final Set<String> _exclusiveAccounts;

  public GoogleAdWordsExtractor(Properties configurations, GoogleAdWordsCredential credential) {
    _configurations = configurations;
    _credential = credential;

    _masterCustomerId = configurations.getProperty(GoogleAdWordsSource.KEY_MASTER_CUSTOMER);
    //TODO: Chen - Update
    String exactAccounts = configurations.getProperty(GoogleAdWordsSource.KEY_ACCOUNTS_EXACT);
    if (exactAccounts == null || exactAccounts.trim().isEmpty()) {
      _exactAccounts = null;
      String exclusiveAccounts = configurations.getProperty(GoogleAdWordsSource.KEY_ACCOUNTS_EXCLUDE);
      if (exclusiveAccounts == null || exclusiveAccounts.trim().isEmpty()) {
        _exclusiveAccounts = null;
      } else {
        _exclusiveAccounts = new HashSet<>(Arrays.asList(exclusiveAccounts.split(",")));
      }
    } else {
      _exactAccounts = new HashSet<>(Arrays.asList(exactAccounts.split(",")));
      _exclusiveAccounts = null;
    }
  }

  public void work()
      throws Exception {
    AdWordsSession.ImmutableAdWordsSession rootSession = _credential.buildRootSession();
    Collection<String> jobs = getConfiguredAccounts(rootSession, _masterCustomerId, _exactAccounts, _exclusiveAccounts);

    GoogleAdWordsReportDownloader reportDownloader = new GoogleAdWordsReportDownloader(rootSession, _configurations);

    reportDownloader.downloadAllReports(jobs);
  }

  /**
   * 1. Get all available non-manager accounts.
   * 2. If exactAccounts are provided, validate that all exactAccounts are a subset of step 1.
   * 3. If exclusiveAccounts are provided, remove them from step 1.
   */
  private static Collection<String> getConfiguredAccounts(AdWordsSession rootSession, String masterCustomerId,
      Set<String> exactAccounts, Set<String> exclusiveAccounts)
      throws ValidationException, RemoteException {

    GoogleAdWordsAccountManager accountManager = new GoogleAdWordsAccountManager(rootSession);
    Map<Long, ManagedCustomer> availableAccounts = accountManager.getChildrenAccounts(masterCustomerId, false);
    Set<String> available = new HashSet<>();
    for (Map.Entry<Long, ManagedCustomer> account : availableAccounts.entrySet()) {
      available.add(Long.toString(account.getKey()));
    }
    LOG.info(
        String.format("Found %d available accounts for your master account %s", available.size(), masterCustomerId));

    if (exactAccounts != null) {
      Sets.SetView<String> difference = Sets.difference(exactAccounts, available);
      if (difference.isEmpty()) {
        return exactAccounts;
      } else {
        String msg = String
            .format("The following accounts configured in the exact list don't exist under master account %s: %s",
                masterCustomerId, Joiner.on(",").join(difference));
        LOG.error(msg);
        throw new RuntimeException(msg);
      }
    }

    if (exclusiveAccounts != null && !exclusiveAccounts.isEmpty()) {
      available.removeAll(exclusiveAccounts);
    }
    return available;
  }
}
