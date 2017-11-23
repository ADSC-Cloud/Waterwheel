#! /bin/bash

function print_help {
    echo "help info:"
    echo "-p" namenode_host
    echo "-m" master/slave
    exit 0
}

while getopts hp:m: option
do
    case "${option}"
    in
      h) print_help;;
      p) NAMENODE_HOST=${OPTARG};;
      m) MODE=${OPTARG};;
    esac
done

if [ -z $NAMENODE_HOST ]; then
    echo "please specify the master node host"
    print_help
    exit 0
else
    echo "namenode host is $NAMENODE_HOST"
fi

if [ -z $MODE ]; then
    echo "please specify the node type: master/slave"
    print_help
    exit 0
fi

if [ ! "$MODE" = "slave" ] && [ ! "$MODE" = "master" ]; then
    echo "parameter -m must be either slave or master"
    exit 0
fi

if [ ! -f hadoop-2.8.1.tar.gz ]; then
    wget http://www.apache.org/dist/hadoop/common/hadoop-2.8.1/hadoop-2.8.1.tar.gz
fi

echo "extracting files..."
echo "type [yes] should you are asked any question.."
tar zxf hadoop-2.8.1.tar.gz

cp default-config/core-site.xml hadoop-2.8.1/etc/hadoop/
cp default-config/hdfs-site.xml hadoop-2.8.1/etc/hadoop/

sed -i "s/namenode-host/$NAMENODE_HOST/g" hadoop-2.8.1/etc/hadoop/core-site.xml

DATANODE_FOLDER=`pwd`/hadoop-2.8.1/datanode-folder
mkdir -p "`pwd`/hadoop-2.8.1/datanode-folder"
echo "data node: $DATANODE_FOLDER"
sed -i "s|datanode-dir|$DATANODE_FOLDER|g" hadoop-2.8.1/etc/hadoop/hdfs-site.xml

# set $JAVA_HOME variable in hadoop-env.sh
sed -i "s|JAVA_HOME=.*|JAVA_HOME=$JAVA_HOME|g" hadoop-2.8.1/etc/hadoop/hadoop-env.sh
if [ "$MODE" = "master" ]; then
    hadoop-2.8.1/bin/hdfs namenode -format
    hadoop-2.8.1/sbin/start-dfs.sh
else
    hadoop-2.8.1/sbin/hadoop-daemons.sh  --script "hadoop-2.8.1/bin/hdfs" start datanode
fi
