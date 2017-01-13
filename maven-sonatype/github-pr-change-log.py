#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


# Usage: Python Script that takes in a range of Gobblin Pull Request Numbers and outputs metadata about each Pull Request
#
# An example output would look like:
#
# * [] [PR 902] Make it possible to specify empty job data publisher
# * [] [PR 903] The underlying Avro CodecFactory only matches lowercase codecs, so we…
# * [] [PR 904] Fixed precondition check for overwriting in datapublisher
#
# The output of this script is meant for the CHANGELOG file that is updated before each Gobblin release.
# There is a pair of [] brackets at the beginning of the build which is meant to containg the project name the PR is related to
#
# The script should be run as follows "./pull-requests-change-log.py [github-username] [github-password] [starting-pr-number] [ending-pr-number]
# For example, to produce the above output the command run was "./pull-requests-change-log.py sahilTakiar [my-password] 900 905"

import requests
import sys

for prNumber in range(int(sys.argv[3]), int(sys.argv[4])):
	pr = requests.get("https://api.github.com/repos/linkedin/gobblin/pulls/" + str(prNumber), auth=(sys.argv[1], sys.argv[2])).json();
	if "state" in pr.keys() and  pr["state"] == "closed" and pr["merged"]:
		print "* [] [PR " + str(pr["number"]) + "] " + pr["title"]
