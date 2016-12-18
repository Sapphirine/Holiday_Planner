#!/bin/bash
#

continent=$1
country=$2
state=$3

START_PATH="${MAHOUT_HOME}/examples/bin"

# Set commands for dfs
source ${START_PATH}/set-dfs-commands.sh

MAHOUT="${MAHOUT_HOME}/bin/mahout"

if [ ! -e $MAHOUT ]; then
  echo "Can't find mahout driver in $MAHOUT, cwd `pwd`, exiting.."
  exit 1
fi

WORK_DIR=/tmp/travelblog
RESULT_DIR=/tmp/travelblog-result
MAHOUT_LOCAL=""
export MAHOUT_LOCAL

$DFS -mkdir -p $WORK_DIR
mkdir -p $RESULT_DIR
#echo "Creating work directory at ${WORK_DIR}"

echo "Converting to Sequence Files from Directory"
$MAHOUT seqdirectory -i ${WORK_DIR}/${continent}/${country}/${state} -o ${WORK_DIR}/${continent}/${country}/${state}/seqdir -c UTF-8 -chunk 64 -xm sequential

$MAHOUT seq2sparse \
  -i ${WORK_DIR}/${continent}/${country}/${state}/seqdir/ \
  -o ${WORK_DIR}/${continent}/${country}/${state}/seqdir-sparse-kmeans --maxDFPercent 85 \
  --namedVector \
&& \
$MAHOUT kmeans \
  -i ${WORK_DIR}/${continent}/${country}/${state}/seqdir-sparse-kmeans/tfidf-vectors/ \
  -c ${WORK_DIR}/${continent}/${country}/${state}/kmeans-clusters \
  -o ${WORK_DIR}/${continent}/${country}/${state}/kmeans \
  -dm org.apache.mahout.common.distance.EuclideanDistanceMeasure \
  -x 10 -k 20 -ow --clustering

inputDir=`$DFS -ls -d ${WORK_DIR}/${continent}/${country}/${state}/kmeans/clusters-*-final | nawk '{print $8}'`

if [ -d ${RESULT_DIR}/${continent}/${country}/${state} ]; then
    rm ${RESULT_DIR}/${continent}/${country}/${state}/*
fi

$MAHOUT clusterdump \
    -i ${inputDir} \
    -o ${RESULT_DIR}/${continent}/${country}/${state}/clusterdump \
    -d ${WORK_DIR}/${continent}/${country}/${state}/seqdir-sparse-kmeans/dictionary.file-0 \
    -dt sequencefile -b 100 -n 20 --evaluate -dm org.apache.mahout.common.distance.EuclideanDistanceMeasure -sp 0 \
    --pointsDir ${WORK_DIR}/${continent}/${country}/${state}/kmeans/clusteredPoints

