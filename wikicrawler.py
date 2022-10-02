import xml.etree.ElementTree as etree
import re
import os
import sqlite3

def strip_tag_name(t):
    idx = t.rfind("}")
    if idx != -1:
        t = t[idx + 1:]
    return t


def main():
    con = sqlite3.connect("wikilinks.db")
    cur = con.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS pages(page, link)")

    PATH_WIKI_XML = '/mnt/d/newest'
    FILENAME_WIKI = 'enwiki-latest-pages-articles-multistream.xml'
    pathWikiXML = os.path.join(PATH_WIKI_XML, FILENAME_WIKI)

    count = 0
    redirect = False
    for event, elem in etree.iterparse(pathWikiXML, events=('start', 'end')):
        tname = strip_tag_name(elem.tag)

        if event == 'start':
            if tname == "title":
                title = elem.text
            if tname == "redirect":
                redirect = True

        elif event == 'end':
            if tname == 'text' and redirect:
                redirect = False
                continue
            elif tname == 'text' and not redirect:
                links = re.findall('(?:\[{2})([\w\s]+)(?:[|\w\s]*)(?:\]{2})', elem.text)
                for link in links:
                    cur.execute("INSERT INTO pages VALUES(?, ?)", (title, link))          

        count += 1
        if count == 1000000:
            print(count)
        con.commit()


