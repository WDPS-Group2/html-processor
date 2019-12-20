ES_PORT=9200
TD_PORT=9200
ES_NODE="localhost"
TD_NODE="localhost"

echo "Zipping local python dependencies"

if [ -f "libs.zip" ]; then
  echo "Local python dependencies archive already exists"
else
  cd src/ && zip -x score.py,spark_runner.py,spark_runner_locally.py -r ../libs.zip . && cd ..
fi

echo "Downloading nltk_data"

cd venv/share
rm -rf nltk_data/
mkdir nltk_data
cd nltk_data
python3 -m nltk.downloader -d ./ maxent_ne_chunker
python3 -m nltk.downloader -d ./ stopwords
python3 -m nltk.downloader -d ./ words
python3 -m nltk.downloader -d ./ averaged_perceptron_tagger
python3 -m nltk.downloader -d ./ punkt
cd ../../..

INFILE=${1:-"./sample.warc.gz"}
OUTFILE=${2:-"extracted-entities.tsv"}

rm -rf $OUTFILE

echo "Running on Spark"

PYSPARK_PYTHON=$(readlink -f $(which python3)) /opt/spark-3.0.0-preview-bin-hadoop2.7/bin/spark-submit \
--conf "spark.pyspark.python=venv/bin/python3" \
--conf "spark.pyspark.driver.python=venv/bin/python3" \
--py-files libs.zip \
src/spark_runner_locally.py $INFILE $OUTFILE $ES_NODE:$ES_PORT $TD_NODE:$TD_PORT




