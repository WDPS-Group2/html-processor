# Setup and running
## Python
This project is configured to run with **Python 3.6**. In order to load Python 3.6 on the DAS4 cluster, please run `module load python/3.6.0`

## Pipenv
Make sure you have pipenv installed. If you don't, you can install it using the following command: `pip install pipenv`.

## Spark
In order to run the scripts within Spark, you need to have Spark installed. You can find more information [here](https://spark.apache.org/docs/latest/).

Before running the script, please set the location of `spark-submit` to the **SPARK_SUBMIT_LOCATION** environment variable. Example:
`export SPARK_SUBMIT_LOCATION=/local/spark/spark-2.4.0-bin-hadoop2.7/bin/spark-submit`
## Running
You can easily run the code within spark by executing the script ***spark-run.sh***. First of all please make sure the 
 script has execution rights and if it doesn't, please execute: `chmod +x spark-run.sh` to allow the script to run.
 
 Before running the script, please make sure you are in the same directory with the script and then, you can execute it as follows:
 `./spark-run.sh [location of warc file]`.




