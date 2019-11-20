from bs4 import BeautifulSoup
import requests

url = "https://www.timeout.com/amsterdam/things-to-do/best-things-to-do-in-amsterdam"
page = requests.get(url)
html_content = page.text
parsed_page = BeautifulSoup(html_content, 'html.parser')

for script in parsed_page(["script", "style"]):
    script.decompose()

raw_page = parsed_page.get_text()

lines = (line.strip() for line in raw_page.splitlines())
chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
# drop blank lines
text = '\n'.join(chunk for chunk in chunks if chunk)
print(text)