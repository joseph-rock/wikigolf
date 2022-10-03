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
                    links = re.findall('(?:\[{2})([\w\s]+)(?:[|\w\s]*)(?:\]{2})', elem.text)
                    block = []
                    for link in links:
                        block.append((title, link))
                        
                    cur.executemany("INSERT INTO pages VALUES(?, ?)", block)          
                    con.commit()
                except TypeError:
                    print(title)
        count += 1
        if count > 100000:
            print(count)
            count = 0
            con.commit
            con.close()
            con = sqlite3.connect("/mnt/d/wikilinks.db")
            cur = con.cursor()
    
    con.commit
    con.close()
            


main()