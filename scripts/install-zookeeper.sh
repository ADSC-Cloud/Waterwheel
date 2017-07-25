#! /bin/bash
wget http://www-eu.apache.org/dist/zookeeper/zookeeper-3.4.9/zookeeper-3.4.9.tar.gz

tar xvf zookeeper-3.4.9.tar.gz

cp zookeeper-3.4.9/conf/zoo_sample.cfg zookeeper-3.4.9/conf/zoo.cfg

cd zookeeper-3.4.9/bin/
./zkServer.sh start
cd ../..
