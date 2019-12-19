ES_PORT=9200
ES_BIN=/home/wdps1902/elasticsearch-2.4.1/bin/elasticsearch

TD_PORT=9090
TD_BIN=/home/jurbani/trident/build/trident
TD_PATH=/home/jurbani/data/motherkb-trident

zip libs.zip html2text.py
zip libs.zip nlp_preproc_spark.py
zip libs.zip elasticsearch.py
zip libs.zip sparql.py

#source venv/bin/activate
#virtualenv --relocatable venv
#zip -r venv.zip venv

echo "Starting elasticsearch on a new node"

>.es_log*
prun -o .es_log -v -t 00:15:00 -np 1 ESPORT=$ES_PORT $ES_BIN </dev/null 2> .es_node &
echo "Waiting for elasticsearch to set up..."
until [ -n "$ES_NODE" ]; do ES_NODE=$(cat .es_node | grep '^:' | grep -oP '(node...)'); done
ES_PID=$!
until [ -n "$(cat .es_log* | grep YELLOW)" ]; do sleep 1; done

echo "Elastichsearch should be running now on node $ES_NODE:$ES_PORT (connected to process $ES_PID)"


echo "Starting Trident on a new node"

prun -o .td_log -v -t 00:15:00 -np 1 $TD_BIN server -i $TD_PATH --port $TD_PORT </dev/null 2> .td_node &
echo "waiting 5 seconds for trident to set up..."
until [ -n "$TD_NODE" ]; do TD_NODE=$(cat .td_node | grep '^:' | grep -oP '(node...)'); done
sleep 5
TD_PID=$!

echo "Trident should be running now on node $TD_NODE:$TD_PORT (connected to process $TD_PID)"

SCRIPT=${1:-"spark.py"}
INFILE=${2:-"/home/wdps1902/data/sample.warc.gz"}
OUTFILE=${3:-"entity-linking-output"}

/local/spark/spark-2.4.0-bin-hadoop2.7/bin/spark-submit \
--conf "spark.pyspark.python=venv/bin/python3" \
--conf "spark.pyspark.driver.python=venv/bin/python3" \
--archives venv.zip#VENV,nltk_data.zip#NLTK_DATA \
--py-files libs.zip \
$SCRIPT $INFILE $OUTFILE $ES_NODE:$ES_PORT $TD_NODE:$TD_PORT

kill $ES_PID
kill $TD_PID