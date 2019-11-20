import warcio as warc
from html_processor import HtmlProcessor

class WarcItem:
    def __init__(self, url, recordId, rawHTML):
        self.url = url
        self.recordId = recordId
        self.rawHTML = rawHTML


class WarcReader:

    @staticmethod
    def get_warc_items(warc_file_iterator):
        warc_records = []
        for record in warc_file_iterator:
            if record.rec_type == 'response' and record.http_headers.get_header('Content-Type') == 'text/html':
                warc_url = record.rec_headers.get_header('WARC-Target-URI')
                warc_id = record.rec_headers.get_header('WARC-Record-ID')
                warc_html = record.content_stream().read()
                warc_records.append(WarcItem(warc_url, warc_id, str(warc_html)))
        return warc_records

    
    @staticmethod
    def convert_warc_to_dataframe(warc_file_location):
        try:
            warc_file = open(warc_file_location, "rb")
            warc_file_it = warc.ArchiveIterator(warc_file)
            warc_items = WarcReader.get_warc_items(warc_file_it)    
        except Exception as e:
            print(e)
            return None
        finally:
            warc_file.close()


if __name__ == "__main__":
    warc_file_location = "/home/corneliu/sample.warc"
    WarcReader.convert_warc_to_dataframe(warc_file_location)


        
            
