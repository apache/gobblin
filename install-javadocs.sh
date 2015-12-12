#!/bin/bash

script_dir=$(dirname $0)
root_dir=$(cd ${script_dir}; echo $PWD)
script_name=$(basename $0)
build_dir=${root_dir}/build

SITE_JAVADOC_DIR=${root_dir}/javadoc

function print_usage() {
	echo -e "$script_name <version>"
	echo
	echo -e "Copies javadocs for specified version to site directory ${SITE_JAVADOC_DIR}."
}

version=$1

if [ -z "$version" ] ; then
	print_usage
	exit 1
fi

version_root=${SITE_JAVADOC_DIR}/${version}
mkdir -p ${version_root}
site_javadoc_index=${version_root}/index.md


cat - > ${site_javadoc_index} << END_OF_HEADER
---
layout: page
title: Gobblin Javadoc
permalink: /javadoc/${version}/
---

# Gobblin Javadoc Packages

END_OF_HEADER

if ${root_dir}/gradlew -PuseHadoop2 javadoc ; then
	(cd ${build_dir}
	for D in * ; do
		src_javadoc_dir=$D/docs/javadoc
		dest_javadoc_dir=${version_root}/$D
		if [ -d ${src_javadoc_dir} ] ; then
			echo "Installing javadocs for $D"
			mkdir -p ${dest_javadoc_dir}
			cp -r ${src_javadoc_dir}/* ${dest_javadoc_dir}/
			echo -e "# [$D]($D)" >> ${site_javadoc_index}
		fi
	done
	cd - > /dev/null)
else
	echo "$script_name: building of javadocs failed"
	exit 2
fi

cat - >> ${site_javadoc_index} << END_OF_FOOTER

END_OF_FOOTER