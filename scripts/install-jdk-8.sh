#! /bin/bash

if [ ! -f jdk-8u162-linux-x64.rpm ]; then
	wget --no-cookies --no-check-certificate --header "Cookie: gpw_e24=http%3A%2F%2Fwww.oracle.com%2F; oraclelicense=accept-securebackup-cookie" "http://download.oracle.com/otn-pub/java/jdk/8u162-b12/0da788060d494f5095bf8624735fa2f1/jdk-8u162-linux-x64.rpm?AuthParam=1516268516_d5d5701d18a54c88b64fa9948af58acb"
fi

sudo yum localinstall jdk-8u162-linux-x64.rpm

echo "export JAVA_HOME=/usr/java/jdk1.8.0_162/" >> ~/.bashrc
source ~/.bashrc
