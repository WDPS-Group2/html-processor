from bs4 import BeautifulSoup
import requests


class HtmlProcessor:
    
    PARSER_TYPE = 'html.parser'

    @staticmethod
    def extract_raw_text_from_html(html_content):
        """
        Strips away all the html tags including the scripts and styles
        """
        parsed_html = BeautifulSoup(html_content, HtmlProcessor.PARSER_TYPE)

        for script in parsed_html(['script', 'style']):
            script.decompose()

        raw_text = parsed_html.get_text()
        
        lines = (line.strip() for line in raw_text.splitlines())
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        raw_text = ' '.join(chunk for chunk in chunks if chunk)

        return raw_text

    @staticmethod
    def extract_raw_text_from_url(url):
        """
        Downloads the html page and strips the page of html elements
        """
        response = requests.get(url)
        if response.status_code == 200:
            return HtmlProcessor.extract_raw_text_from_html(response.text)
        else:
            print("Could not get webpage, status code: %d" % response.status_code)
            return ""
