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

STARTSTOP=$1

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

# get nephele config
. "$bin"/nephele-config.sh

if [ "$NEPHELE_PID_DIR" = "" ]; then
        NEPHELE_PID_DIR=/tmp
fi

if [ "$NEPHELE_IDENT_STRING" = "" ]; then
        NEPHELE_IDENT_STRING="$USER"
fi

JVM_ARGS="$JVM_ARGS -Xmx2560m"

# auxilliary function to construct a lightweight classpath for the Sopremo server
constructSopremoClassPath() {

	for jarfile in $NEPHELE_LIB_DIR/*.jar ; do

		add=0

		if [[ "$jarfile" =~ 'nephele-common' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'nephele-management' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'pact-common' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'pact-compiler' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'pact-clients' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'pact-runtime' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'sopremo-common' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'sopremo-base' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'sopremo-server' ]]; then
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
		elif [[ "$jarfile" =~ 'guava' ]]; then
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
		elif [[ "$jarfile" =~ 'fastutil' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'javolution' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'nephele-common' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'nephele-management' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'nephele-hdfs' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'nephele-s3' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'nephele-profiling' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'nephele-queuescheduler' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'nephele-clustermanager' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'nephele-ec2cloudmanager' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'nephele-streaming' ]]; then
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
		elif [[ "$jarfile" =~ 'pact-common' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'pact-runtime' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'jackson' ]]; then
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
		elif [[ "$jarfile" =~ 'httpcore' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'httpclient' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'aws-java-sdk' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'guava' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'sopremo-common' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'fastutil' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'javolution' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'javolution' ]]; then
			add=1
		elif [[ "$jarfile" =~ 'commons' ]]; then			
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
	elif [[ "$jarfile" =~ 'xercesImpl' ]]; then
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
			if [[ $PACT_WF_CLASSPATH = "" ]]; then
				PACT_WF_CLASSPATH=$jarfile;
			else
				PACT_WF_CLASSPATH=$PACT_WF_CLASSPATH:$jarfile
			fi
		fi
	done

	echo $PACT_WF_CLASSPATH
}

SOPREMO_SERVER_CLASSPATH=$(constructSopremoClassPath)

log=$NEPHELE_LOG_DIR/sopremo.log
pid=$NEPHELE_PID_DIR/sopremo-server.pid
log_setting="-Dlog.file="$log" -Dlog4j.configuration=file://"$NEPHELE_CONF_DIR"/log4j.properties"

case $STARTSTOP in

        (start)
                mkdir -p "$NEPHELE_PID_DIR"
                if [ -f $pid ]; then
                        if kill -0 `cat $pid` > /dev/null 2>&1; then
                                echo Sopremo server running as process `cat $pid`.  Stop it first.
                                exit 1
                        fi
                fi
                echo Starting Sopremo server
		$JAVA_HOME/bin/java $JVM_ARGS $log_setting -classpath $SOPREMO_SERVER_CLASSPATH eu.stratosphere.sopremo.server.SopremoServer -configDir $NEPHELE_CONF_DIR &
		echo $! > $pid
	;;

        (stop)
                if [ -f $pid ]; then
                        if kill -0 `cat $pid` > /dev/null 2>&1; then
                                echo Stopping Sopremo server
                                kill `cat $pid`
                                kill -9 `cat $pid`
                        else
                                echo No Sopremo server to stop
                        fi
                else
                        echo No Sopremo server to stop
                fi
        ;;

        (*)
                echo Please specify start or stop
        ;;

esac


