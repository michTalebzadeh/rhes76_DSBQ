#!/bin/bash
#
# These notes are for creating sample XML data for broadcast testing
#
#--------------------------------------------------------------------------------
##
## Procedure:    generate_XML.sh
##
## Description:  Generates random XML data based on format supplied by LBG/Novneet Kumar dated 17 Apr 2020
##
## Parameters:   -N < Number_of_rows>
##
## example:       generate_XML.sh -N 10
##
## The resultant xml file is put under the directory that the shell script run. It is called brodcast_test.XML
## It is then added to the hdfs directory /tmp replacing the already existing xml file
##--------------------------------------------------------------------------------
## Vers|  Date  | Who | DA | Description
##-----+--------+-----+----+-----------------------------------------------------
## 1.0 |17/04/20  MT |    | Initial Version
##--------------------------------------------------------------------------------
#
function F_USAGE
{
   echo "USAGE: ${1##*/} -N '< Number of sample xml rows to generate'"
   echo "USAGE: ${1##*/} -H '<HELP>' -h '<HELP>'"
   exit 10
}
#
function genrandom_xml {
SMS_CAMPAIGN_CODE[0]="TRM01"
SMS_CAMPAIGN_CODE[1]="TRM02"
SMS_CAMPAIGN_CODE[2]="TRM03"
SMS_CAMPAIGN_CODE[3]="TRM04"
SMS_CAMPAIGN_CODE[4]="TRM05"
SMS_CAMPAIGN_CODE[5]="TRM06"
SMS_CAMPAIGN_CODE[6]="TRM07"
SMS_CAMPAIGN_CODE[7]="TRM08"
SMS_CAMPAIGN_CODE[8]="TRM09"
SMS_CAMPAIGN_CODE[9]="TRM10"
#
SMS_REQUEST_EXTERNAL_REF[0]="57326670"
SMS_REQUEST_EXTERNAL_REF[1]="57326660"
SMS_REQUEST_EXTERNAL_REF[2]="89745287"
SMS_REQUEST_EXTERNAL_REF[3]="76432178"
SMS_REQUEST_EXTERNAL_REF[4]="98654123"
SMS_REQUEST_EXTERNAL_REF[5]="87612348"
SMS_REQUEST_EXTERNAL_REF[6]="54986541"
SMS_REQUEST_EXTERNAL_REF[7]="99986541"
SMS_REQUEST_EXTERNAL_REF[8]="43561234"
SMS_REQUEST_EXTERNAL_REF[9]="63871987"
#
SMS_REQUEST_EXTERNAL_TXN_REF[0]="181397493749"
SMS_REQUEST_EXTERNAL_TXN_REF[1]="999999988875"
SMS_REQUEST_EXTERNAL_TXN_REF[2]="654787653219"
SMS_REQUEST_EXTERNAL_TXN_REF[3]="754321678985"
SMS_REQUEST_EXTERNAL_TXN_REF[4]="453987654129"
SMS_REQUEST_EXTERNAL_TXN_REF[5]="856432987541"
SMS_REQUEST_EXTERNAL_TXN_REF[6]="765487612387"
SMS_REQUEST_EXTERNAL_TXN_REF[7]="654987654312"
SMS_REQUEST_EXTERNAL_TXN_REF[8]="876098732167"
SMS_REQUEST_EXTERNAL_TXN_REF[9]="342165489654"
#
SMS_TEMPLATE_CODE[0]="C19BCSH"
SMS_TEMPLATE_CODE[1]="C19BCSW"
SMS_TEMPLATE_CODE[2]="C19BCSF"
SMS_TEMPLATE_CODE[3]="C19BCSE"
SMS_TEMPLATE_CODE[4]="C19BCSL"
SMS_TEMPLATE_CODE[5]="C19BCSQ"
SMS_TEMPLATE_CODE[6]="C19BCSQ"
SMS_TEMPLATE_CODE[7]="C19BCSM"
SMS_TEMPLATE_CODE[8]="C19BCSB"
SMS_TEMPLATE_CODE[9]="C19BCSD"
#
BRAND_CODE[0]="MBN"
BRAND_CODE[1]="HAL"
BRAND_CODE[2]="BOS"
BRAND_CODE[3]="LTB"
#
OCIS_MRG_PTY_ID[0]="1156516468"
OCIS_MRG_PTY_ID[1]="7863421239"
OCIS_MRG_PTY_ID[2]="8798543217"
OCIS_MRG_PTY_ID[3]="1098765432"
OCIS_MRG_PTY_ID[4]="3276543219"
OCIS_MRG_PTY_ID[5]="7609876543"
OCIS_MRG_PTY_ID[6]="8765098543"
OCIS_MRG_PTY_ID[7]="5437687691"
OCIS_MRG_PTY_ID[8]="7656453411"
OCIS_MRG_PTY_ID[9]="1111165498"
#
TARGET_MOBILE_NO[0]="7411224959"
TARGET_MOBILE_NO[1]="7786098111"
TARGET_MOBILE_NO[2]="9965438761"
TARGET_MOBILE_NO[3]="7654309871"
TARGET_MOBILE_NO[4]="7658761121"
TARGET_MOBILE_NO[5]="9876543210"
TARGET_MOBILE_NO[6]="765908711"
TARGET_MOBILE_NO[7]="7983123456"
TARGET_MOBILE_NO[8]="7654398793"
TARGET_MOBILE_NO[9]="7760987652"

   NUMBER=$[RANDOM % ${#SMS_CAMPAIGN_CODE[@]}]
   SIGNAL1=${SMS_CAMPAIGN_CODE[$NUMBER]}
   NUMBER=$[RANDOM % ${#SMS_REQUEST_EXTERNAL_REF[@]}]
   SIGNAL2=${SMS_REQUEST_EXTERNAL_REF[$NUMBER]}
   NUMBER=$[RANDOM % ${#SMS_REQUEST_EXTERNAL_TXN_REF[@]}]
   SIGNAL3=${SMS_REQUEST_EXTERNAL_TXN_REF[$NUMBER]}
   NUMBER=$[RANDOM % ${#SMS_TEMPLATE_CODE[@]}]
   SIGNAL4=${SMS_TEMPLATE_CODE[$NUMBER]}
   NUMBER=$[RANDOM % ${#BRAND_CODE[@]}]
   SIGNAL5=${BRAND_CODE[$NUMBER]}
   NUMBER=$[RANDOM % ${#OCIS_MRG_PTY_ID[@]}]
   SIGNAL6=${OCIS_MRG_PTY_ID[$NUMBER]}
   NUMBER=$[RANDOM % ${#TARGET_MOBILE_NO[@]}]
   SIGNAL7=${TARGET_MOBILE_NO[$NUMBER]}

cat >> $XML_FILE << !
<sms_request>
<sms_campaign_code>${SIGNAL1}</sms_campaign_code>
<target_mobile_no>${SIGNAL7}</target_mobile_no>
<sms_template_code>${SIGNAL4}</sms_template_code>
<sms_request_external_ref>${SIGNAL2}</sms_request_external_ref>
<sms_request_external_txn_ref>${SIGNAL3}</sms_request_external_txn_ref>
<sms_template_variables>
  <brand_code>${SIGNAL5}</brand_code>
  <ocis_mrg_pty_id>${SIGNAL6}</ocis_mrg_pty_id>
</sms_template_variables>
</sms_request>
!
}
#
# Main Section
#
if [[ "${1}" = "-h" || "${1}" = "-H" ]]; then
   F_USAGE $0
fi
## MAP INPUT TO VARIABLES
while getopts N: opt
do
   case $opt in
   (N) N="$OPTARG" ;;
   (*) F_USAGE $0 ;;
   esac
done
#
[[ -z ${N} ]] && echo "You must specify the number of rows to create in the sample XML file" && F_USAGE $0
NOW="`date +%Y%m%d`"
#
LOGDIR="./"
#
## For iest purposes it expects the XML file to be in the current directory and pushes the XML file to hdfs tmp/directory with smale XML filename
HDFS_DIR="hdfs://shared-pas-08.sandbox.local:8020"
#
FILE_NAME=`basename $0 .sh`
XML_FILE="${LOGDIR}/broadcast_test.XML"
[ -f ${XML_FILE} ] && rm -f ${XML_FILE}
LOG_FILE="${LOGDIR}/${FILE_NAME}.log"
[ -f ${LOG_FILE} ] && rm -f ${LOG_FILE}
echo `date` " ""======= Started creating $N rows of test XML broadcast data ======="  | tee -a ${LOG_FILE}
cat >> $XML_FILE << !
<sms_requests>
!
integer ROWCOUNT=1
while (( ROWCOUNT <= $N))
do
    genrandom_xml
    ((ROWCOUNT = ROWCOUNT + 1 ))
done
cat >> $XML_FILE << !
</sms_requests>
!
#
### Put the generated XML file into HDFS
# 
hadoop fs -test -f ${HDFS_DIR}/tmp/${XML_FILE}
if [ $? = 0 ]
then
  hdfs dfs -rm -r ${HDFS_DIR}/tmp/${XML_FILE}
fi
hdfs dfs -copyFromLocal ${XML_FILE} ${HDFS_DIR}/tmp
if [ $? != 0 ]
then
     echo `date` " ""======= Abort could not copy the sample xml file ${XML_FILE} into ${HDFS_DIR}/tmp directory ======" | tee -a  ${LOG_FILE}
     exit 1
else
     echo `date` " ""======= Copied sample xml file ${XML_FILE} into ${HDFS_DIR}/tmp directory ======"  | tee -a ${LOG_FILE}
     echo `date` " ""======= Completed creating $N rows of test XML broadcast data ======="  | tee -a ${LOG_FILE}
fi
exit 0
