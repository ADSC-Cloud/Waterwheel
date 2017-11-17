This folder contains the scripts to initilize the cluster environment, designed for Ubuntu 16.04 started from a clear image.

Run the scripts in the following order:

1. generate security key and added authorized keys
2. install JDK 8 and config $JAVA_HOME
3. install and deploy hdfs
4. install zookeeper
5. install storm

The scripts should be run on both master node and the slave nodes.

#### 1. Generate security key
On both master and slave nodes:

```./generate-public-key.sh```

#### 2. Add public key to the authorized_keys

On master node:

```./add-authorized-keys.sh ~/.ssh/id_rsa.pub```

On slave nodes:

```./add-authorized-keys.sh ~/.ssh/id_rsa.pub```

Copy id_rsa.pub from the master into a local file master-key.pub and run the following command to add it to the authorized keys:

```./add-authorized-keys.sh master-key.pub```

#### 3. Install JDK and set $JAVA_PATH

On both master and slave nodes:

```. ./install-jdk-8.sh```

After this step, ```java -version``` should be run correctly and ```echo $JAVA_HOME$``` should print the location of jdk

#### 4. Install and deploy HDFS

On master node:

Replace MASTER_IP with the actual ip

```./install-hdfs.sh -m master -p MASTER_IP```

On slave node:

Replace MASTER_IP with the actual ip

```./install-hdfs.sh -m slave -p MASTER_IP```

#### 5. Install and deploy Zookeeper

On master node only:

```./install-zookeeper.sh```

#### 6. Install and deploy Storm

On master node:

Replace MASTER_IP with the actual ip

```./install-storm.sh -m master -p MASTER_IP```

On slave node:

Replace MASTER_IP with the actual ip

```./install-storm.sh -m slave -p MASTER_IP```

#### 7. Deplay Waterwheel topology
On master node:

Replace MASTER_IP with the actual ip and NUMBER_OF_NODE with the actual value, e.g., 5

```./deploy-topology.sh -p MASTER_IP -n NUMBER_OF_NODE```

#### 8. Deploy Waterwheel web ui

On master node:

```./deploy-tomcat.sh```
