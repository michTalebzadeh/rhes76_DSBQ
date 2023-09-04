#!/bin/bash
#--------------------------------------------------------------------------------
#
# Procedure:    run_spark_jobs
#
# Description:  Run scala app with spark-submit
#
# Parameters:   E -> Environment, M-> Mode
#
##  Example:	run_spark_jobs.sh -E sbdev -M local
##		run_spark_jobs.sh -E sbdev -M cluster

#
#--------------------------------------------------------------------------------
# Vers|  Date  | Who | DA | Description
#-----+--------+-----+----+-----------------------------------------------------
# 1.0 |26/04/20|  MT |    | Initial Version
#--------------------------------------------------------------------------------
#
function F_USAGE
{
   echo "USAGE: ${1##*/} -E '<Env>'"
   echo "USAGE: ${1##*/} -M '<Mode>'"
   echo "USAGE: ${1##*/} -H '<HELP>' -h '<HELP>'"
   exit 10
}
#
function default_settings {
#
envFile="../envconfigs/${ENV}/common.properties"
if [[ -f ${envFile} ]]
then
  echo "Loading environment file ${envFile}"
  source ${envFile}
  echo "source success"
  # get a ticket if one is not already obtained
  echo $principal
  user=`whoami`
  cache_file_name="/tmp/krb5cc_pas_$user"
  echo "Kerberos Cache Ticket File : $cache_file_name"
  export KRB5CCNAME="$cache_file_name"
  echo $keytab
  kinit -c FILE:$cache_file_name  $principal -kt $keytab
  ls -ltr $cache_file_name
  klist
  echo "Environment file loaded: $envFile"
else
  echo "Error, Cannot load environment file ${envFile} for environment **${ENV}**, Quitting"
  exit 1
fi
#
SPARK_HOME="/usr/hdp/current/spark2-client"
JAR_FILES="/usr/hdp/current/hbase-client/lib/hbase-client.jar,/usr/hdp/current/hbase-client/lib/hbase-common.jar,/usr/hdp/current/hbase-client/lib/hbase-server.jar,/usr/hdp/current/hbase-client/lib/guava-12.0.1.jar,/usr/hdp/current/hbase-client/lib/hbase-protocol.jar,/usr/hdp/current/hbase-client/lib/htrace-core-3.1.0-incubating.jar,/usr/hdp/current/spark-client/lib/datanucleus-api-jdo-3.2.6.jar,/usr/hdp/current/spark-client/lib/datanucleus-rdbms-3.2.9.jar,/usr/hdp/current/spark-client/lib/datanucleus-core-3.2.10.jar,/usr/hdp/current/hive-client/lib/hive-hbase-handler-1.2.1000.2.6.4.69-1.jar,/usr/hdp/current/phoenix-client/lib/phoenix-spark-4.7.0.2.6.4.69-1.jar,/usr/hdp/current/phoenix-client/phoenix-server.jar"
FILES="/etc/spark2/conf/hive-site.xml,/etc/spark2/conf/hbase-site.xml,../envconfigs/${ENV}/hbase.properties,../envconfigs/${ENV}/common.properties,../envconfigs/${ENV}/broadcastStagingConfig.properties"
SCHEDULER="spark.scheduler.mode=FIFO"
EXTRAJAVAOPTIONS="spark.executor.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps"
}
function run_local {
default_settings

${SPARK_HOME}/bin/spark-submit \
	 --name "PAS_BROADCAST_STAGING_JOB" \
	 --master local \
         --num-executors 1 \
         --executor-memory 1G \
         --configs spark.driver.extraJavaOptions="-Dlog4j.configuration=../envconfigs/$env/log4j.properties" \
         --configs spark.executor.extraJavaOptions="-Dlog4j.configuration=../envconfigs/$env/log4j.properties" \
	 --class com.lbg.pas.alerts.bigdata.brst.app.BroadcastStagingApp \
         --jars ${JAR_FILES} \
	 --files ${FILES} \
         --verbose ../jars/broadcast-staging-release.jar ${ENV} local "../envconfigs/"
}
#
function run_cluster {
default_settings

/usr/hdp/current/spark2-client/bin/spark-submit \
	 --name "PAS_BROADCAST_STAGING_JOB" \
	 --master yarn \
         --deploy-mode cluster \
         --driver-memory 6GB \
	 --executor-cores 2  \
         --num-executors 4 \
         --executor-memory 8G \
         --configs "${SCHEDULER}" \
         --configs spark.driver.extraJavaOptions="-Dlog4j.configuration=../envconfigs/$env/log4j.properties" \
         --configs spark.executor.extraJavaOptions="-Dlog4j.configuration=../envconfigs/$env/log4j.properties" \
	 --class com.lbg.pas.alerts.bigdata.brst.app.BroadcastStagingApp \
         --jars ${JAR_FILES} \
	 --files ${FILES} \
         --verbose ../jars/broadcast-staging-release.jar ${ENV} cluster
}
#
# Main Section
#
if [[ "${1}" = "-h" || "${1}" = "-H" ]]; then
   F_USAGE $0
fi
## MAP INPUT TO VARIABLES
while getopts E:M: opt
do
   case $opt in
   (E) ENV="$OPTARG" ;;
   (M) MODE="$OPTARG" ;;
   (*) F_USAGE $0 ;;
   esac
done

[[ -z ${ENV} ]] && echo "You must specify a valid environment under ../envconfigs " && F_USAGE $0
MODE=`echo ${MODE}|tr "[:upper:]" "[:lower:]"`
if [[ "${MODE}" != "local" ]]  && [[ "${MODE}" != "cluster" ]]
then
        echo "Incorrect value for run mode. The run mode can only be local or cluster"  && F_USAGE $0
fi

FILE_NAME=`basename $0 .sh`
NOW="`date +%Y%m%d_%H%M`"
LOG_FILE=./${FILE_NAME}.log
[ -f ${LOG_FILE} ] && rm -f ${LOG_FILE}
touch ${LOG_FILE}
#
default_settings

echo `date` ", Running in **${MODE} mode** for Environment **${ENV}**" | tee -a ${LOG_FILE}

if [[ "${MODE}" = "local" ]]
then
	run_local
elif [[ "${MODE}" = "cluster" ]]
then
	run_cluster
fi

echo `date` ", Finished $0" | tee -a ${LOG_FILE}
#
exit
