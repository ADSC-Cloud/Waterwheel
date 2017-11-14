#! /bin/bash
wget http://www-us.apache.org/dist/zookeeper/zookeeper-3.4.11/zookeeper-3.4.11.tar.gz

tar xvf zookeeper-3.4.11.tar.gz

cp zookeeper-3.4.11/conf/zoo_sample.cfg zookeeper-3.4.11/conf/zoo.cfg

cd zookeeper-3.4.11/bin/
./zkServer.sh start
cd ../..
