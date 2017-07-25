#! /bin/bash

function print_help {
    echo "help info:"
    echo "-p" namenode_host
    exit 0
}

while getopts hp: option
do
    case "${option}"
    in
      h) print_help;;
      p) NAMENODE_HOST=${OPTARG};;
    esac
done

if [ -z $NAMENODE_HOST ]; then
    echo "please specify the master node host"
    print_help
    exit 0
else
    echo "namenode host is $NAMENODE_HOST"
fi

if [ ! -f hadoop-2.7.3.tar.gz ]; then
    wget http://www-eu.apache.org/dist/hadoop/common/hadoop-2.7.3/hadoop-2.7.3.tar.gz
fi

tar zxf hadoop-2.7.3.tar.gz

cp default-config/core-site.xml hadoop-2.7.3/etc/hadoop/
cp default-config/hdfs-site.xml hadoop-2.7.3/etc/hadoop/

sed -i "s/namenode-host/$NAMENODE_HOST/g" hadoop-2.7.3/etc/hadoop/core-site.xml
hadoop-2.7.3/bin/hdfs namenode -format

DATANODE_FOLDER=`pwd`/hadoop-2.7.3/datanode-folder
mkdir -p "`pwd`/hadoop-2.7.3/datanode-folder"
echo "data node: $DATANODE_FOLDER"
sed -i "s|datanode-dir|$DATANODE_FOLDER|g" hadoop-2.7.3/etc/hadoop/hdfs-site.xml

# set $JAVA_HOME variable in hadoop-env.sh
sed -i "s|JAVA_HOME=.*|JAVA_HOME=$JAVA_HOME|g" hadoop-2.7.3/etc/hadoop/hadoop-env.sh

hadoop-2.7.3/sbin/start-dfs.sh
