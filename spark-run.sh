if [[ -z "${SPARK_DIR}" ]]; then
  SPARK_SUBMIT="/opt/spark-3.0.0-preview-bin-hadoop2.7/bin/spark-submit"
else
  SPARK_SUBMIT="${SPARK_DIR}/bin/spark-submit"
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
dist/main.py -f $1