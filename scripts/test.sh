sed -i "s|JAVA_HOME=.*|JAVA_HOME=$JAVA_HOME|g" hadoop-2.7.3/etc/hadoop/hadoop-env.sh 
echo "JAVA_HOME=*$JAVA_HOME"

cat hadoop-2.7.3/etc/hadoop/hadoop-env.sh
