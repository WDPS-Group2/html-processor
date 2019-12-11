import argparse
import os
import time
import sys

if os.path.exists('libs.zip'):
    sys.path.insert(0, 'libs.zip')

from warc import warc_checker
from spark_handler.spark_executor import SparkExecutor


def is_valid_file(parser, filename):
    if not os.path.exists(filename):
        parser.error("The file %s does not exist!" % filename)
    elif not warc_checker.is_file_valid_warc_file(filename):
        parser.error("The file %s is not a .gz archive" % filename)
    return filename


def parse_arguments():
    parser = argparse.ArgumentParser(description='Warc processor')
    parser.add_argument("-f", "--filename", dest="filename",
                        required=True, help="warc file archive",
                        metavar="FILE", type=lambda x: is_valid_file(parser, x))

    args = parser.parse_args()
    return args


start_time = time.time()

args = parse_arguments()
filename = args.filename

warc_df = SparkExecutor.get_spark_dataframe_for_warc_filename(filename)
warc_entities_df = SparkExecutor.extract_entities_from_warc_spark_df(warc_df)
warc_entities_df.show(100, False)

duration = time.time() - start_time
print("Execution duration: %.2fs" % duration)

