#!/bin/bash

set -e
set -o pipefail

# These environment variables would normally be set by Spark scripts
# However, for a Databricks init script, they have not been set yet.
# We will keep the names the same here, but not export them.
# These must be changed if the associated Spark environment variables
# are changed.
DB_HOME=/databricks
SPARK_HOME=$DB_HOME/spark
SPARK_CONF_DIR=$SPARK_HOME/conf

cat << 'EOF' >> "$SPARK_CONF_DIR/spark-env.sh"

export DB_CLUSTER_ID=$DB_CLUSTER_ID
export DB_CLUSTER_NAME=$DB_CLUSTER_NAME
export LOG_ANALYTICS_WORKSPACE_ID=$LOG_ANALYTICS_WORKSPACE_ID
export LOG_ANALYTICS_WORKSPACE_KEY=$LOG_ANALYTICS_WORKSPACE_KEY
export AZ_SUBSCRIPTION_ID=$AZ_SUBSCRIPTION_ID
export AZ_RSRC_GRP_NAME=$AZ_RSRC_GRP_NAME
export AZ_RSRC_PROV_NAMESPACE=$AZ_RSRC_PROV_NAMESPACE
export AZ_RSRC_TYPE=$AZ_RSRC_TYPE
export AZ_RSRC_NAME=$AZ_RSRC_NAME

# Note: All REGEX filters below are implemented with java.lang.String.matches(...).  This implementation essentially appends ^...$ around
# the regular expression, so the entire string must match the regex.  If you need to allow for other values you should include .* before and/or
# after your expression.

# Add a quoted regex value to filter the events for SparkListenerEvent_CL, the log will only include events where Event_s matches the regex.
# Filter the events for SparkListenerEvent_CL, the log will only include events where Event_s ends with SparkListenerApplicationStart
export LA_SPARKLISTENEREVENT_REGEX=".*SparkListenerApplicationStart$"

# Add a quoted regex value to filter the events for SparkMetric_CL, the log will only include events where name_s matches the regex.
# Only log metrics where the name begins with app and ends in .jvmCpuTime or .heap.max.
# export LA_SPARKMETRIC_REGEX="app.*\.jvmCpuTime|app.*\.heap.max"

# Only log spark metrics where the names contain 'driver.executor.bytesRead' or 'driver.executor.bytesWritten'
export LA_SPARKMETRIC_REGEX="driver\.executor\.bytes(Read|Written)"

# Add a quoted regex value to filter the events for SparkLoggingEvent_CL, the log will only include events where logger_name_s matches the name regex
# or where the Message matches the message regex.  If both are specified, then both must be matched for the log to be sent.
# Commented examples below will only log messages where the logger name is com.microsoft.pnp.samplejob.StreamingQueryListenerSampleJob or begins with
# org.apache.spark.util.Utils, or where the Message ends with the string 'StreamingQueryListenerSampleJob' or begins with the string 'FS_CONF_COMPAT'.
# export LA_SPARKLOGGINGEVENT_NAME_REGEX="com\.microsoft\.pnp\.samplejob\.StreamingQueryListenerSampleJob|org\.apache\.spark\.util\.Utils.*"
# export LA_SPARKLOGGINGEVENT_MESSAGE_REGEX=".*StreamingQueryListenerSampleJob|FS_CONF_COMPAT.*"

# Only log messages where the logger name starts with UnifiedLogger
export LA_SPARKLOGGINGEVENT_NAME_REGEX="^UnifiedLogger.*$"

# Uncomment the following line to enable local logging to stderr of buffer contents when an exception is hit when sending a buffer to Log Analytics
# export LA_LOGFAILEDBUFFERSEND=TRUE

EOF

STAGE_DIR=/dbfs/databricks/spark-monitoring
SPARK_MONITORING_VERSION=${SPARK_MONITORING_VERSION:-1.0.0}
SPARK_VERSION=$(cat /databricks/spark/VERSION 2> /dev/null || echo "")
SPARK_VERSION=${SPARK_VERSION:-3.4.1}
SPARK_SCALA_VERSION=$(ls /databricks/spark/assembly/target | cut -d '-' -f2 2> /dev/null || echo "")
SPARK_SCALA_VERSION=${SPARK_SCALA_VERSION:-2.12}

# This variable configures the spark-monitoring library metrics sink.
# Any valid Spark metric.properties entry can be added here as well.
# It will get merged with the metrics.properties on the cluster.
METRICS_PROPERTIES=$(cat << EOF
# This will enable the sink for all of the instances.
*.sink.loganalytics.class=org.apache.spark.metrics.pnp.LogAnalyticsMetricsSink
*.sink.loganalytics.period=5
*.sink.loganalytics.unit=seconds

# Enable JvmSource for instance master, worker, driver and executor
master.source.jvm.class=org.apache.spark.metrics.source.JvmSource
worker.source.jvm.class=org.apache.spark.metrics.source.JvmSource
driver.source.jvm.class=org.apache.spark.metrics.source.JvmSource
executor.source.jvm.class=org.apache.spark.metrics.source.JvmSource

EOF
)

JAR_FILENAME="spark-monitoring_${SPARK_MONITORING_VERSION}.jar"
echo "Copying $JAR_FILENAME"
cp -f "$STAGE_DIR/$JAR_FILENAME" /mnt/driver-daemon/jars
echo "Copied Spark Monitoring jars successfully"

echo "Copying log4j-layout-template-json-2.17.2.jar"
wget --quiet -O /mnt/driver-daemon/jars/log4j-layout-template-json-2.17.2.jar https://repo1.maven.org/maven2/org/apache/logging/log4j/log4j-layout-template-json/2.17.2/log4j-layout-template-json-2.17.2.jar
echo "Copied log4j-layout-template-json-2.17.2.jar"

echo "Merging metrics.properties"
echo "$(echo "$METRICS_PROPERTIES"; cat "$SPARK_CONF_DIR/metrics.properties")" > "$SPARK_CONF_DIR/metrics.properties" || { echo "Error writing metrics.properties"; exit 1; }
echo "Merged metrics.properties successfully"

# This will enable master/worker metrics
cat << EOF >> "$SPARK_CONF_DIR/spark-defaults.conf"
spark.metrics.conf ${SPARK_CONF_DIR}/metrics.properties
EOF

log4jDirectories=( "executor" "driver" "master-worker" )
for log4jDirectory in "${log4jDirectories[@]}"
do

LOG4J_CONFIG_FILE="$SPARK_HOME/dbconf/log4j/$log4jDirectory/log4j2.xml"
echo "BEGIN: Updating $LOG4J_CONFIG_FILE with Log Analytics appender"
CONTENT="\ \ <LogAnalyticsAppender name=\"logAnalyticsAppender\">\n\ \ \ \ <JsonTemplateLayout eventTemplateUri=\"file:${STAGE_DIR}/sparkLayout.json\"/>\n\ \ </LogAnalyticsAppender>"
C=$(echo $CONTENT | sed 's/\//\\\//g')
sed -i "/<\/Appenders>/ s/.*/${C}\n&/" $LOG4J_CONFIG_FILE

CONTENT="\ \ \ \ \ \ <AppenderRef ref=\"logAnalyticsAppender\"/>"
C=$(echo $CONTENT | sed 's/\//\\\//g')
sed -i "/<\/Root>/ s/.*/${C}\n&/" $LOG4J_CONFIG_FILE

sed -i 's/packages="\([^"]*\)"/packages="\1,com.microsoft.pnp.logging.loganalytics"/' $LOG4J_CONFIG_FILE

echo "END: Updating $LOG4J_CONFIG_FILE with Log Analytics appender"

done

# The spark.extraListeners property has an entry from Databricks by default.
# We have to read it here because we did not find a way to get this setting when the init script is running.
# If Databricks changes the default value of this property, it needs to be changed here.
cat << EOF > "$DB_HOME/driver/conf/00-custom-spark-driver-defaults.conf"
[driver] {
    "spark.extraListeners" = "com.databricks.backend.daemon.driver.DBCEventLoggingListener,com.microsoft.pnp.listeners.DatabricksListener"
}
EOF


cat << 'EOF' > "$STAGE_DIR/sparkLayout.json"
{
  "@timestamp": {
    "$resolver": "timestamp",
    "pattern": {
      "format": "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",
      "timeZone": "UTC"
    }
  },
  "ecs.version": "1.2.0",
  "log.level": {
    "$resolver": "level",
    "field": "name"
  },
  "message": {
    "$resolver": "message",
    "stringified": true
  },
  "process.thread.name": {
    "$resolver": "thread",
    "field": "name"
  },
  "log.logger": {
    "$resolver": "logger",
    "field": "name"
  },
  "labels": {
    "$resolver": "mdc",
    "flatten": true,
    "stringified": true
  },
  "tags": {
    "$resolver": "ndc"
  },
  "error.type": {
    "$resolver": "exception",
    "field": "className"
  },
  "error.message": {
    "$resolver": "exception",
    "field": "message"
  },
  "error.stack_trace": {
    "$resolver": "exception",
    "field": "stackTrace",
    "stackTrace": {
      "stringified": true
    }
  },
  "sparkAppName": "${spark:spark.app.name:-}",
  "sparkNode": "${spark:nodeType}"
}
EOF

