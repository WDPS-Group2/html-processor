ES_PORT=9200
ES_BIN=/home/wdps1902/elasticsearch-2.4.1/bin/elasticsearch

TD_PORT=9090
TD_BIN=/home/jurbani/trident/build/trident
TD_PATH=/home/jurbani/data/motherkb-trident

echo "Starting elasticsearch on a new node"

>.es_log*
prun -o .es_log -v -t 00:15:00 -np 1 ESPORT=$ES_PORT $ES_BIN </dev/null 2> .es_node &
echo "Waiting for elasticsearch to set up..."
until [ -n "$ES_NODE" ]; do ES_NODE=$(cat .es_node | grep '^:' | grep -oP '(node...)'); done
ES_PID=$!
until [ -n "$(cat .es_log* | grep YELLOW)" ]; do sleep 1; done

echo "Elastichsearch should be running now on node $ES_NODE:$ES_PORT (connected to process $ES_PID)"

#echo "Starting Trident on a new node"
#
#prun -o .td_log -v -t 00:15:00 -np 1 $TD_BIN server -i $TD_PATH --port $TD_PORT </dev/null 2> .td_node &
#echo "waiting 5 seconds for trident to set up..."
#until [ -n "$TD_NODE" ]; do TD_NODE=$(cat .td_node | grep '^:' | grep -oP '(node...)'); done
#sleep 5
#TD_PID=$!
#
#echo "Trident should be running now on node $TD_NODE:$TD_PORT (connected to process $TD_PID)"

if [[ -z "${WDPS_SPARK_DIR}" ]]; then
  SPARK_SUBMIT="/local/spark/spark-2.4.0-bin-hadoop2.7/bin/spark-submit"
else
  SPARK_SUBMIT="${WDPS_SPARK_DIR}/bin/spark-submit"
fi

echo "Using the following spark-submit executable: ${SPARK_SUBMIT}"

if (( $# != 1)); then
  echo "The warc file is required as input"
  exit
fi

echo "Installing all pipenv dependencies!"

pipenv install
rm -rf dist/
mkdir dist/
cp src/main.py dist/
cd src/ && zip -x main.py -r ../dist/libs.zip . && cd ..

echo "Running the WDPS entity linking within Spark!"

export PYTHONPATH=/home/jurbani/trident/build-python

$SPARK_SUBMIT \
--conf "spark.pyspark.python=`pipenv --py`" \
--conf "spark.pyspark.driver.python=`pipenv --py`" \
--py-files dist/libs.zip \
dist/main.py -f $1 -esHost $ES_NODE -esPort $ES_PORT -tdHost mock -tdPort $TD_PORT

echo "Killing Elasticsearch"
kill $ES_PID
#echo "Killing Trident"
#kill $TD_PID
