from bs4 import BeautifulSoup, Comment


def record2html(record):
    ishtml = False
    html = ""
    for line in record.splitlines():
        if line.startswith("<html"):
            ishtml = True
        if ishtml:
            html += line
    return html


def html2text(record):
    html_doc = record2html(record)
    useless_tags = ['footer', 'header', 'sidebar', 'sidebar-right', 'sidebar-left', 'sidebar-wrapper', 'wrapwidget', 'widget']
    if html_doc:
        soup = BeautifulSoup(html_doc,"html.parser")

        [s.extract() for s in soup(['script','style', 'code','title','head','footer','header'])]
        [s.extract() for s in soup.find_all(id = useless_tags)]
        [s.extract() for s in soup.find_all(name='div',attrs={"class": useless_tags})]

        for element in soup(s=lambda s: isinstance(s, Comment)):
            element.extract()

        paragraph = soup.find_all("p")
        text = ""
        for p in paragraph:
            if p.get_text(" ", strip=True) != '':
                text += p.get_text(" ", strip=True)+"\n"
        if text ==  "":
            text = soup.get_text(" ", strip=True)
        return text
    return ""
