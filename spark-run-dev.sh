ES_HOST="127.0.0.1"
ES_PORT="9200"

if [[ -z "${WDPS_SPARK_DIR}" ]]; then
  SPARK_SUBMIT="/opt/spark-3.0.0-preview-bin-hadoop2.7/bin/spark-submit"
else
  SPARK_SUBMIT="${WDPS_SPARK_DIR}/bin/spark-submit"
fi

if (( $# != 1)); then
  echo "The warc file is required as input"
  exit
fi

pipenv install
rm -rf dist/
mkdir dist/
cp src/main.py dist/
cd src/ && zip -x main.py -r ../dist/libs.zip . && cd ..

$SPARK_SUBMIT \
--conf "spark.pyspark.python=`pipenv --py`" \
--conf "spark.pyspark.driver.python=`pipenv --py`" \
--py-files dist/libs.zip \
dist/main.py -f $1 -esHost $ES_HOST -esPort $ES_PORT