#!/bin/bash

if [ ! -s waterwheel.war ]; then
    cp ../web/target/webapp.war waterwheel.war
    echo "waterwheel.war is generated."
else
    echo "found waterwheel.war file"
fi


if [ ! -s apache-tomcat-8.5.27.tar.gz ]; then
    wget http://www-us.apache.org/dist/tomcat/tomcat-8/v8.5.27/bin/apache-tomcat-8.5.27.tar.gz
else
    echo "apache-tomcat-8.5.27.tar.gz is found."
fi

tar xf apache-tomcat-8.5.27.tar.gz
echo "tomcat is extracted from tar.gz file"

cp waterwheel.war apache-tomcat-8.5.27/webapps/
echo "waterwheel.war is copied into tomcat../webapps/"
apache-tomcat-8.5.27/bin/startup.sh

echo "You can visit the web ui via http://localhost:8080/waterwheel."
echo "Enjoy it!" 
