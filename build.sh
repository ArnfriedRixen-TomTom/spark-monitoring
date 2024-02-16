#! /bin/bash -eu

# Fix broken DNS bits
echo 'hosts: files dns' > /etc/nsswitch.conf
echo "127.0.0.1   $(hostname)" >> /etc/hosts

MAVEN_PROFILES=( "dbr-11.3-lts" "dbr-12.2-lts")
for MAVEN_PROFILE in "${MAVEN_PROFILES[@]}"
do
    mvn -f /spark-monitoring/pom.xml install -P ${MAVEN_PROFILE} "$@"
done

# mvn package -P "dbr-13.3-lts"         
# dbfs ls dbfs:/databricks/spark-monitoring/         
# dbfs rm --recursive dbfs:/databricks/spark-monitoring/       
# dbfs mkdirs dbfs:/databricks/spark-monitoring 
# databricks fs cp /Users/rixen/projects/ArnfriedRixen-TomTom/spark-monitoring/target/spark-monitoring_1.0.0.jar dbfs:/databricks/spark-monitoring/spark-monitoring_1.0.0.jar