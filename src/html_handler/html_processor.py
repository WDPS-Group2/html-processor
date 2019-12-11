from bs4 import BeautifulSoup, Comment
import requests

PARSER_TYPE = 'html.parser'


def extract_raw_text_from_html(html_content):
    """
    Extracts raw text from an html page by also filtering navigation items, scripts, styles and others
    """
    parsed_html = BeautifulSoup(html_content, PARSER_TYPE)

    title_element = parsed_html.find("title")
    title = title_element.get_text(strip=True) if title_element is not None else None

    """ Removes html tags that have an id containing the word nav """
    [item.decompose() for item in parsed_html.findAll("div", id=lambda x: x and "nav" in x)]

    """ Removes html tags that have a class containing the word nav """
    [item.decompose() for item in parsed_html.findAll("div", class_=lambda x: x and "nav" in x)]

    """ Removes all script, style, title and head items from the html """
    [item.decompose() for item in parsed_html(['script', 'style', 'code', 'title', 'head'])]

    """ Removes all the items that have the id footer or header """
    [item.decompose() for item in parsed_html.find_all(id=['footer', 'header'])]

    """ Removes all the comments from the HTML"""
    [item.decompose() for item in parsed_html(s=lambda s: isinstance(s, Comment))]

    raw_text = parsed_html.get_text()

    lines = (line.strip() for line in raw_text.splitlines())
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    raw_text = ' '.join(chunk for chunk in chunks if chunk)

    output = title + ' ' + raw_text if title is not None else raw_text
    return output


def extract_raw_text_from_url(url):
    """
    Downloads the html page and strips the page of html elements
    """
    response = requests.get(url)
    if response.status_code == 200:
        return extract_raw_text_from_html(response.text)
    else:
        print("Could not get webpage, status code: %d" % response.status_code)
        return ""
