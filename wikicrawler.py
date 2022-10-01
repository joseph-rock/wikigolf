import xml.etree.ElementTree as etree
import codecs
import csv
import time
import os
import sys

PATH_WIKI_XML = 'D:/newest/'
FILENAME_WIKI = 'enwiki-latest-pages-articles-multistream.xml'
FILENAME_ARTICLES = 'articles.csv'
FILENAME_REDIRECT = 'articles_redirect.csv'
FILENAME_TEMPLATE = 'articles_template.csv'
ENCODING = "utf-8"


print(os.path)
pathWikiXML = os.path.join(PATH_WIKI_XML, FILENAME_WIKI)

for event, elem in etree.iterparse(pathWikiXML, events=('start', 'end')):
    print(elem)
    break