import xml.etree.ElementTree as etree
import re
import os
import sqlite3

import time
import progressbar

def strip_tag_name(t):
    idx = t.rfind("}")
    if idx != -1:
        t = t[idx + 1:]
    return t


def db(con, cur, block):
    cur.executemany("INSERT INTO pages VALUES(?, ?)", block)          
    con.commit()

def xmlParse():
    PATH_WIKI_XML = '/mnt/d/newest'
    FILENAME_WIKI = 'enwiki-latest-pages-articles-multistream.xml'
    pathWikiXML = os.path.join(PATH_WIKI_XML, FILENAME_WIKI)

    con = sqlite3.connect("/mnt/d/wikilinks.db")
    cur = con.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS pages(page, link)")

    # old
    # REGEX = r'(?:\[\[)([^\[\]\|]+)(?:\|[^\[\]]+)?(?:\]\])'
    # new
    REGEX = r'(?:\[\[)([^\[\]]+?)(?:\|[^\[\]]*)?(?:\]\])'
    
    bar = progressbar.ProgressBar(max_value=progressbar.UnknownLength)
    i = 0
    for _, elem in etree.iterparse(pathWikiXML):
        tname = strip_tag_name(elem.tag)

        if tname == 'title':
            title = elem.text
        elif tname == 'text' and elem.text[:9] != '#REDIRECT':
            text = elem.text
            try: 
                matches = re.finditer(REGEX, text)
                block = []
                for match in matches:
                    block.append((title, match.group(1)))
                cur.executemany("INSERT INTO pages VALUES(?, ?)", block)          
                con.commit()
                
                bar.update(i)
                i += 1
                    
            except TypeError:
                print(f'Failed on: {title}')

        elem.clear()
        

    con.close()


if __name__ == '__main__':
    xmlParse()
    