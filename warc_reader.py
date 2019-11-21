import warcio as warc
import pandas as pd
from html_processor import HtmlProcessor


class WarcReader:

    @staticmethod
    def get_warc_items(warc_file_iterator):
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
        try:
            warc_file = open(warc_file_location, "rb")
            warc_file_it = warc.ArchiveIterator(warc_file)
            warc_items = WarcReader.get_warc_items(warc_file_it) 
            warc_dataframe = pd.DataFrame(data=list(warc_items), columns=['id', 'url', 'html'])
            warc_file.close()
            return warc_dataframe
        except Exception as e:
            print(e)
            return None


if __name__ == "__main__":
    warc_file_location = "/home/corneliu/sample.warc"
    warc_df = WarcReader.convert_warc_to_dataframe(warc_file_location)
    warc_df['html'] = warc_df['html'].apply(HtmlProcessor.extract_raw_text_from_html)
    print(warc_df)
