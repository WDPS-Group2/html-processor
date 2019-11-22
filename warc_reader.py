from warc_checker import WarcChecker

import warcio as warc
import pandas as pd
import traceback
import sys


class WarcReader:

    @staticmethod
    def get_warc_items(warc_file_iterator):
        """
        Iterates over all the warc pages and adds the ids, the urls, and the html code to separate arrays
        """
        warc_urls = []
        warc_ids = []
        warc_htmls = []

        for record in warc_file_iterator:
            if record.rec_type == 'response' and record.http_headers.get_header('Content-Type') == 'text/html':
                warc_urls.append(record.rec_headers.get_header('WARC-Target-URI'))
                warc_ids.append(record.rec_headers.get_header('WARC-Record-ID'))
                warc_htmls.append(record.content_stream().read())
         
        return zip(warc_ids, warc_urls, warc_htmls)

    @staticmethod
    def convert_warc_to_dataframe(warc_file_location):
        """
        Converts a warc file to a pandas Dataframe
        """
        try:
            warc_file_location = WarcReader.check_warc_format_and_get_updated_location(warc_file_location)
            warc_file = open(warc_file_location, "rb")
            warc_file_it = warc.ArchiveIterator(warc_file)
            warc_items = WarcReader.get_warc_items(warc_file_it) 
            warc_dataframe = pd.DataFrame(data=list(warc_items), columns=['id', 'url', 'html'])
            warc_file.close()
            return warc_dataframe
        except Exception as e:
            traceback.print_exc(file=sys.stdout)
            return None

    @staticmethod
    def check_warc_format_and_get_updated_location(warc_file_location):
        """
        Checks the payload and block digests of WARC records. If the WARC is invalid a recompression will be attempted.
        If that also fails, an exception is thrown, because we can't work with with invalid WARC files
        """
        if WarcChecker.is_warc_file_in_correct_format(warc_file_location):
            print("Provided file is in valid WARC format")
            return warc_file_location

        print("Provided Warc file is not in correct format, attempting recompression!")
        warc_file_location = WarcChecker.recompress_warc_file_in_correct_format(warc_file_location)

        if warc_file_location is not None:
            return warc_file_location

        sys.exit(-1)
