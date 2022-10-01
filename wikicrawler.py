import xml.etree.ElementTree as etree
import os

PATH_WIKI_XML = '/mnt/d/newest'
FILENAME_WIKI = 'enwiki-latest-pages-articles-multistream.xml'
FILENAME_ARTICLES = 'articles.csv'
FILENAME_REDIRECT = 'articles_redirect.csv'
FILENAME_TEMPLATE = 'articles_template.csv'
ENCODING = "utf-8"

pathWikiXML = os.path.join(PATH_WIKI_XML, FILENAME_WIKI)

def strip_tag_name(t):
    idx = t.rfind("}")
    if idx != -1:
        t = t[idx + 1:]
    return t

count = 0
for event, elem in etree.iterparse(pathWikiXML, events=('start', 'end')):
    tname = strip_tag_name(elem.tag)

    print(tname, elem.text)

    elem.clear()

    count += 1
    if count == 1000:
        break