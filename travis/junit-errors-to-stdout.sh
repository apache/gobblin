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

#
# Script used by Travis builds upon a failure to format and
# print the failing test results to the console.
#

#!/bin/bash
IFS='
'
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
ROOTDIR="$1"
if [ -z "$ROOTDIR" ]; then
	ROOTDIR="."
fi
echo 'Formatting results...'
FILES=$(find "$ROOTDIR" -path '*/build/*/test-results/*.xml' | python "$DIR/filter-to-failing-test-results.py")
if [ -n "$FILES" ]; then
	for file in $FILES; do
		echo "Formatting $file"
		if [ -f "$file" ]; then
			echo '====================================================='
			xsltproc "$DIR/junit-xml-format-errors.xsl" "$file"
		fi
	done
	echo '====================================================='
else
	echo 'No */build/*/test-results/*.xml files found with failing tests.'
fi
