from bs4 import BeautifulSoup
import requests


class HtmlProcessor:
    
    PARSER_TYPE = 'html.parser'

    @staticmethod
    def extract_raw_text_from_html(html_content):
        """ Strips away all the html tags including the script and style content """
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
        response = requests.get(url)
        if response.status_code == 200:
            return HtmlProcessor.extract_raw_text_from_html(response.text)
        else:
            print("Could not get webpage, status code: %d" % response.status_code)
            return ""
    

if __name__ == "__main__":
    test_url = "https://www.timeout.com/amsterdam/things-to-do/best-things-to-do-in-amsterdam"
    raw_text = HtmlProcessor.extract_raw_text_from_url(test_url)
    print(raw_text)