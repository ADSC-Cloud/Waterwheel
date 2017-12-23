#! /bin/bash

if [ ! -f jdk-8u152-linux-x64.rpm ]; then
	wget --no-cookies --no-check-certificate --header "Cookie: gpw_e24=http%3A%2F%2Fwww.oracle.com%2F; oraclelicense=accept-securebackup-cookie" "http://download.oracle.com/otn-pub/java/jdk/8u152-b16/aa0333dd3019491ca4f6ddbe78cdb6d0/jdk-8u152-linux-x64.rpm"
fi

sudo yum localinstall jdk-8u152-linux-x64.rpm

echo "export JAVA_HOME=/usr/java/jdk1.8.0_152/" >> ~/.bashrc
source ~/.bashrc
