#!/bin/bash
#--------------------------------------------------------------------------------
#
# Procedure: 	create_tables.sh
#
# Description:	create hive broadcast tables
#
# Parameters:   E -> Environment
#
##  Example:    create_tables.sh  -E sbdev
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
   echo "USAGE: ${1##*/} -H '<HELP>' -h '<HELP>'"
   exit 10
}
function default_settings {
#
envFile="../envconfigs/${ENV}/broadcastStagingConfig.properties"
if [[ -f ${envFile} ]]
then
  echo "Loading environment file ${envFile}"
  source ${envFile}
  echo "source success"
  # get a ticket if one is not already obtained
  echo $principalName
  user=`whoami`
  cache_file_name="/tmp/krb5cc_pas_$user"
  echo "Kerberos Cache Ticket File : $cache_file_name"
  export KRB5CCNAME="$cache_file_name"
  echo $keytabFile
  kinit -c FILE:$cache_file_name  $principalName -kt $keytabFile
  ls -ltr $cache_file_name
  klist
  echo "Environment file loaded: $envFile"
else
  echo "Error, Cannot load environment file ${envFile} for environment **${ENV}**, Quitting"
  exit 1
fi
#
}
broadcastStagingConfig.broadcastTable=
broadcastStagingConfig.broadcastExceptionTableName=e

DROP TABLE IF EXISTS ${broadcastStagingConfig.broadcastTable};
CREATE TABLE IF NOT EXISTS ${broadcastStagingConfig.broadcastTable} (
           partyId STRING
         , phoneNumber STRING
)
        PARTITIONED BY (
           broadcastId STRING
         , brand STRING
)
ROW FORMAT SERDE
   'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
;
DROP TABLE IF EXISTS ${broadcastStagingConfig.broadcastExceptionTableName};
CREATE TABLE IF NOT EXISTS ${broadcastStagingConfig.broadcastExceptionTableName} (
           sms_campaign_code STRING
         , sms_request_external_ref LONG
         , sms_request_external_txn_ref LONG
         , sms_template_code STRING
         , partyId STRING
         , phoneNumber STRING
         , whenRejected DATE
         , reason STRING
)
        PARTITIONED BY (
           broadcastId STRING
         , brand STRING
)
ROW FORMAT SERDE
   'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
;
