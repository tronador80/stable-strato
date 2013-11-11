#bin/sh
~/s/bin/meteor-webfrontend.sh stop
~/s/bin/stop-sopremo-server.sh
~/s/bin/stop-local.sh
#cd /home/dopa-user/dopasphere/stratgosphere-dist/
cd /home/dopa-user/mleich-dev/dopa
#mvn clean
#mvn clean package || exit 1
mvn clean package -Dmaven.test.skip=true
#echo "transmitting to dopa-2"
#scp -r ~/s dopa-2:~
~/s/bin/start-local.sh
~/s/bin/start-sopremo-server.sh
~/s/bin/meteor-webfrontend.sh start > /dev/null 2>&1
#hadoop fs -rm -r /user/dopa-user/wordCount
echo "wait 1 seconds"
#sleep 5
#~/s/bin/pact-client.sh run -w -j /home/dopa-user/mleich-dev/dopa/pact/pact-examples/target/pact-examples-0.2.1-WordCount.jar -a 32 hdfs://dopa-1.dima.tu-berlin.de:8020/testdata/textdata/hamlet.txt hdfs://dopa-1.dima.tu-berlin.de:8020/user/dopa-user/wordCount
echo "wait a second"
#sleep 1
#hadoop fs -cat /user/dopa-user/wordCount/*
~/s/bin/nephele-visualization.sh
#~/s/bin/meteor-client.sh ~/Desktop/test.meteor --wait
#echo "Meteor submitted"
#cat ~/s/log/nephele-dopa-user-taskmanager-dopa-1.dima.tu-berlin.de.out