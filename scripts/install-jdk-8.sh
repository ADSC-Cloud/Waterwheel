#! /bin/bash

sudo add-apt-repository ppa:webupd8team/java

sudo apt update; sudo apt install oracle-java8-installer -y

sudo apt install oracle-java8-set-default -y

echo "export JAVA_HOME=/usr/lib/jvm/java-8-oracle" >> ~/.bashrc
source ~/.bashrc
