from multiprocessing import Queue, Process
import xml.etree.ElementTree as etree
import re
import os
import sqlite3
import logging
import time
import progressbar

def strip_tag_name(t):
    idx = t.rfind("}")
    if idx != -1:
        t = t[idx + 1:]
    return t


def sql_worker(q):
    con = sqlite3.connect("/mnt/d/wikilinks.db")
    cur = con.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS pages(page, link)")

    print(os.getpid(),"working")
    while True:
        block = q.get()

        if block is None:
            break

        cur.executemany("INSERT INTO pages VALUES(?, ?)", block)          
        con.commit()

    con.close()
    print(os.getpid(),"finished")


def xml_worker(q):
    PATH_WIKI_XML = '/mnt/d/newest'
    FILENAME_WIKI = 'enwiki-latest-pages-articles-multistream.xml'
    PATH = os.path.join(PATH_WIKI_XML, FILENAME_WIKI)
    REGEX = r'(?:\[\[)([^\[\]]+?)(?:\|[^\[\]]*)?(?:\]\])'

    bar = progressbar.ProgressBar(max_value=progressbar.UnknownLength)
    i = 0
    block = []

    logging.basicConfig(filename="std.log", filemode='a', format='%(asctime)s %(message)s')
    logging.error("Start")
    
    for _, elem in etree.iterparse(PATH):
        tname = strip_tag_name(elem.tag)

        if tname == 'title':
            title = elem.text
        elif tname == 'text':
            text = elem.text
            try: 
                if elem.text[:9] == '#REDIRECT':
                    continue
                matches = re.finditer(REGEX, text)
                for match in matches:
                    block.append((title, match.group(1)))

                if len(block) > 10000:
                    q.put(block)
                    block = []

                bar.update(i)
                i += 1
                    
            except TypeError:
                logging.error(f'Failed on: {title}\n{elem.text}')

            except:
                logging.error(f"Something else happened: {title}\n{elem.text}")

        elem.clear()
        
    if len(block) > 0:
        q.put(block)
    q.put(None)

    logging.error("Finish")


if __name__ == '__main__':
    print(os.getpid(),"working")
    con = sqlite3.connect("/mnt/d/wikilinks.db")
    cur = con.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS pages(page, link)")
    con.close()

    sqlQ = Queue(1000)

    xmlP = Process(target=xml_worker, args=(sqlQ,))
    sqlP = Process(target=sql_worker, args=(sqlQ,))
    
    xmlP.start()
    sqlP.start()

    sqlQ.close()
    sqlQ.join_thread()

    xmlP.join()
    sqlP.join()