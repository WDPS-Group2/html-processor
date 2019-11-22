import argparse
import os
from warc_reader import WarcReader
from warc_checker import WarcChecker
from html_processor import HtmlProcessor


def is_valid_file(parser, filename):
    if not os.path.exists(filename):
        parser.error("The file %s does not exist!" % filename)
    elif not WarcChecker.is_file_valid_warc_file(filename):
        parser.error("The file %s is not a .gz archive" % filename)
    return filename


def parse_arguments():
    parser = argparse.ArgumentParser(description='Warc processor')
    parser.add_argument("-f", "--filename", dest="filename",
                        required=True, help="warc file archive",
                        metavar="FILE", type=lambda x: is_valid_file(parser, x))

    args = parser.parse_args()
    return args


def execute():
    args = parse_arguments()
    filename = args.filename
    warc_df = WarcReader.convert_warc_to_dataframe(filename)
    warc_df['html'] = warc_df['html'].apply(HtmlProcessor.extract_raw_text_from_html)
    print(warc_df)


execute()
