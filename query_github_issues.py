#!/bin/env /usr/bin/python2.6

from datetime import date,timedelta
import json
import httplib
from string import Template
import sys
import urllib

def search_issues(query_terms, sortBy=None, orderDir=None):
  conn = httplib.HTTPSConnection("api.github.com")
  params_map = {"q": "+".join(query_terms)}
  if None != sortBy:
    params_map["sort"] = sortBy
  if None != orderDir:
    params_map["order"] = orderDir
  # Note we don't do urlencode because the output is not compatible with the q syntax, e.g. : and / should not be escaped
  # params = urllib.urlencode(params_map)
  params = "&".join(str(i[0]) + "=" + str(i[1]) for i in params_map.items())
  #print params

  headers = {"User-Agent": "Python App"}

  conn.request("GET", "/search/issues?" + params, headers=headers)
  response = conn.getresponse()
  if response.status != httplib.OK:
    sys.stderr.write("Query error: %s %s: %s" % (response.status, response.reason, response.read()))
    sys.exit(1)
  result = json.loads(response.read())
  conn.close()
  return result


def get_created_issues_since(day):
  return search_issues(query_terms=["repo:linkedin/gobblin", "is:open", "is:issue", "created:>=" + day], 
                      sortBy="created",
                      orderDir="desc"
                     )

def get_created_issues_last_days(n = 10):
  since_day = (date.today() - timedelta(days = n)).strftime("%Y-%m-%d") 
  return get_created_issues_since(since_day)

def simple_issue_list(issues):
  HEADER_TEMPLATE = Template("$total_count issues found")
  ISSUE_TEMPLATE = Template("""------------------
ISSUE $number : HTML: $html_url  JSON: $url
\tCREATED ON: $created_at
\tCREATED BY: $user_login ( $user_name ) $user_html_url
\tASSIGNED TO: $assignee_login ($assignee_name) $assignee_html_url
\tCOMMENTS: $comments
\tUPDATED ON: $updated_at
\tCLOSED ON: $closed_at

$body
""")
  print HEADER_TEMPLATE.substitute(issues)
  for issue in issues["items"]:
    user_data = issue["user"]
    for user_attr in user_data:
      issue["user_" + user_attr] = user_data[user_attr]
    assignee_data = issue["assignee"]
    for assignee_attr in assignee_data:
      issue["assignee_" + assignee_attr] = assignee_data[assignee_attr]
    print ISSUE_TEMPLATE.safe_substitute(issue)

def main(argv):
  issues = get_created_issues_last_days(7)
  print simple_issue_list(issues)

if __name__ == "__main__":
  main(sys.argv[1:])
