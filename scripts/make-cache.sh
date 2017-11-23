#!/bin/bash

CHECKSUM="md5sum"


if [[ "$OSTYPE" == "darwin"* ]]; then
    CHECKSUM="md5 -q"
fi

echo "checking hadoop-2.8.1.tar.gz ..."
if [ -s hadoop-2.8.1.tar.gz ] && [[ `$CHECKSUM hadoop-2.8.1.tar.gz |awk '{print $1}'` = "def2211ee1561871794e1b0eec8cb628" ]]; then
    echo "file exists and checksum passed"
else
    echo "downloading hadoop-2.8.1 ..."
    rm -f hadoop-2.8.1.tar.gz
    wget http://www.apache.org/dist/hadoop/common/hadoop-2.8.1/hadoop-2.8.1.tar.gz
fi


echo "checking jdk-8u152-linux-x64.rpm ..."
if [ -s jdk-8u152-linux-x64.rpm ] && [[ `$CHECKSUM jdk-8u152-linux-x64.rpm |awk '{print $1}'` = "b6979be30bdc4077dc93cd99134ad84d" ]]; then
    echo "file exists and checksum passed"
else
    echo "downloading jdk-8u152-linux-x64.rpm ..."
    rm -f jdk-8u152-linux-x64.rpm
    wget --no-cookies --no-check-certificate --header "Cookie: gpw_e24=http%3A%2F%2Fwww.oracle.com%2F; oraclelicense=accept-securebackup-cookie" "http://download.oracle.com/otn-pub/java/jdk/8u152-b16/aa0333dd3019491ca4f6ddbe78cdb6d0/jdk-8u152-linux-x64.rpm"
fi

echo "checking apache-storm-1.1.0.tar.gz ..."
if [ -s apache-storm-1.1.0.tar.gz ] && [[ `$CHECKSUM apache-storm-1.1.0.tar.gz |awk '{print $1}'` = "531294419a45ceb639db065c5b007bd4" ]]; then
    echo "file exists and checksum passed"
else
    echo "downloading apache-storm-1.1.0.tar.gz ..."
    rm -f apache-storm-1.1.0.tar.gz
    wget http://www-us.apache.org/dist/storm/apache-storm-1.1.0/apache-storm-1.1.0.tar.gz
fi

echo "checking  zookeeper-3.4.11.tar.gz..."
if [ -s zookeeper-3.4.11.tar.gz ] && [[ `$CHECKSUM zookeeper-3.4.11.tar.gz |awk '{print $1}'` = "55aec6196ed9fa4c451cb5ae4a1f42d8" ]]; then
    echo "file exists and checksum passed"
else
    echo "downloading zookeeper-3.4.11.tar.gz"
    rm -f zookeeper-3.4.11.tar.gz
    wget http://www-us.apache.org/dist/zookeeper/zookeeper-3.4.11/zookeeper-3.4.11.tar.gz
fi

echo "checking apache-tomcat-8.5.23.tar.gz..."
if [ -s apache-tomcat-8.5.23.tar.gz ] && [[ `$CHECKSUM apache-tomcat-8.5.23.tar.gz |awk '{print $1}'` = "c4addea2c8c166530f11bdeb4730c26e" ]]; then
    echo "file exists and checksum passed"
else
    echo "downloading apache-tomcat-8.5.23.tar.gz"
    wget http://www-us.apache.org/dist/tomcat/tomcat-8/v8.5.23/bin/apache-tomcat-8.5.23.tar.gz
fi

echo "generating/updating waterwheel-topology.jar ..."
cd ..
mvn clean install -DskipTests
cd scripts
cp ../topology/target/topology-1.0-SNAPSHOT.jar waterwheel-topology.jar
echo "waterwheel-topology.jar is updated."

echo "generating/updating waterwheel.war ..."
cp ../web/target/webapp.war waterwheel.war
echo "waterwheel.war is updated."
