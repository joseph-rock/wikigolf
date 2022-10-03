from typing import List
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
    con = sqlite3.connect("/mnt/d/wikilinks.db")
    cur = con.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS pages(page, link)")

    PATH_WIKI_XML = '/mnt/d/newest'
    FILENAME_WIKI = 'enwiki-latest-pages-articles-multistream.xml'
    REGEX = r'(?:\[\[)([^\[\]\|]+)(?:\|[^\[\]]+)?(?:\]\])'
    pathWikiXML = os.path.join(PATH_WIKI_XML, FILENAME_WIKI)
    redirect = False

    count = 0
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
                try: 
                    matches = re.finditer(REGEX, elem.text)
                    block = []
                    for match in matches:
                        block.append((title, match.group(1)))
                        
                    cur.executemany("INSERT INTO pages VALUES(?, ?)", block)          
                    con.commit()
                    elem.clear()
                except TypeError:
                    print(title)

        # count += 1
        # if count > 10000:
        #     break
            # print(count)
            # count = 0
            # con.close()
            # con = sqlite3.connect("/mnt/d/wikilinks.db")
            # cur = con.cursor()
    
    con.commit()
    con.close()
            


main()