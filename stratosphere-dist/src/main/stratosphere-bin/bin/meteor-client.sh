#!/bin/bash
########################################################################################################################
# 
#  Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
# 
#  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
#  the License. You may obtain a copy of the License at
# 
#      http://www.apache.org/licenses/LICENSE-2.0
# 
#  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
#  an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
#  specific language governing permissions and limitations under the License.
# 
########################################################################################################################

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

# get nephele config
. "$bin"/nephele-config.sh

if [ "$NEPHELE_IDENT_STRING" = "" ]; then
        NEPHELE_IDENT_STRING="$USER"
fi

JVM_ARGS="$JVM_ARGS -Xmx2560m"

# auxilliary function to construct a lightweight classpath for the
# Meteor CLI client
constructMeteorCLIClientClassPath() {

	for jarfile in $NEPHELE_LIB_DIR/*.jar ; do

		add=0

		if [[ "$jarfile" =~ 'nephele-server' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'nephele-common' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'nephele-management' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'nephele-hdfs' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'pact-clients' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'pact-common' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'pact-runtime' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'pact-compiler' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'commons-codec' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'commons-httpclient' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'commons-cli' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'commons-logging' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'commons-configuration' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'commons-lang' ]]; then
      add=1
  	elif [[ "$jarfile" =~ 'log4j' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'slf4j-api' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'protobuf-java' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'hadoop-core' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'hadoop-annotations' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'hadoop-auth' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'hadoop-common' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'hadoop-hdfs' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'hadoop-core' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'sopremo' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'meteor' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'antlr' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'fastutil' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'javolution' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'kryo' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'reflectasm' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'minlog' ]]; then
 		    add=1
		elif [[ "$jarfile" =~ 'asm' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'objenesis' ]]; then
            add=1
		elif [[ "$jarfile" =~ 'guava' ]]; then
            add=1
		elif [[ "$jarfile" =~ 'pact-hbase' ]]; then
            add=1
        elif [[ "$jarfile" =~ 'fbem-ens' ]]; then
            add=1
        elif [[ "$jarfile" =~ 'simmetrics' ]]; then
            add=1
        elif [[ "$jarfile" =~ 'jena' ]]; then
            add=1
        elif [[ "$jarfile" =~ 'json' ]]; then
            add=1
        elif [[ "$jarfile" =~ 'okkam' ]]; then
            add=1
	elif [[ "$xercesImpl" =~ 'okkam' ]]; then
            add=1
		elif [[ "$jarfile" =~ 'sopremo-pawel' ]]; then
        add=1
    elif [[ "$jarfile" =~ 'pawel-model' ]]; then
        add=1
    elif [[ "$jarfile" =~ 'anna-3.3' ]]; then
        add=1
    elif [[ "$jarfile" =~ 'index-import' ]]; then
        add=1	
		fi

		if [[ "$add" = "1" ]]; then
			if [[ $METEOR_CC_CLASSPATH = "" ]]; then
				METEOR_CC_CLASSPATH=$jarfile;
			else
				METEOR_CC_CLASSPATH=$METEOR_CC_CLASSPATH:$jarfile
			fi
		fi
	done

	echo $METEOR_CC_CLASSPATH
}

METEOR_CC_CLASSPATH=$(constructMeteorCLIClientClassPath)

log=$NEPHELE_LOG_DIR/meteor.log
log_setting="-Dlog.file="$log" -Dlog4j.configuration=file://"$NEPHELE_CONF_DIR"/log4j.properties"

export NEPHELE_CONF_DIR

$JAVA_HOME/bin/java $JVM_ARGS $log_setting -classpath $METEOR_CC_CLASSPATH eu.stratosphere.meteor.client.CLClient -configDir $NEPHELE_CONF_DIR $*
