function print_help {
    echo "help info:"
    echo "-p namenode_host"
    echo "-n number of nodes in the cluster"
    exit 0
}

while getopts hp:n: option
do
    case "${option}"
    in
      h) print_help;;
      p) NAMENODE_HOST=${OPTARG};;
      n) NODE=${OPTARG};;
    esac
done

if [ -z $NAMENODE_HOST ] || [ -z $NODE ]; then
    echo "please specify the master node host and the number of node"
    print_help
    exit 0
fi



cd ..
#mvn clean install -DskipTests
cd scripts

CURRENT_PATH=`pwd`

METADATA_PATH="$CURRENT_PATH/metadata/"

mkdir -p $METADATA_PATH

CHUNK_PATH="$CURRENT_PATH/data/"
hadoop-2.7.3/bin/hdfs dfs -mkdir -p $CHUNK_PATH


cp default-config/topology-conf.yaml ./topology-conf.yaml
sed -i "s|data-chunk-folder|$CHUNK_PATH|g" topology-conf.yaml

sed -i "s|metadata-folder|$METADATA_PATH|g" topology-conf.yaml

sed -i "s|namenode-host|$NAMENODE_HOST|g" topology-conf.yaml


apache-storm-1.1.0/bin/storm jar ../topology/target/topology-1.0-SNAPSHOT.jar indexingTopology.topology.kingbase.KingBaseTopology -f topology-conf.yaml -m submit -n $NODE -t Waterwheel
