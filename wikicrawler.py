from multiprocessing import Queue, Process
import xml.etree.ElementTree as etree
import re
import os
import sqlite3
import logging
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

    logging.error(f"SQL {os.getpid()} started")
    while True:
        block = q.get()

        if block is None:
            break

        cur.executemany("INSERT INTO pages VALUES(?, ?)", block)          
        con.commit()

    con.close()
    logging.error(f"SQL {os.getpid()} finished")


def reg_worker(regQ, sqlQ):
    REGEX = r'(?:\[\[)([^\[\]]+?)(?:\|[^\[\]]*)?(?:\]\])'
    block = []

    logging.error(f"Regex {os.getpid()} started")
    while True:
        try:
            title, text = regQ.get()

        except TypeError:
            logging.error(f"Regex {os.getpid()} found type None")
            break

        try: 
            if text[:9] == '#REDIRECT':
                continue
            matches = re.finditer(REGEX, text)
            for match in matches:
                block.append((title, match.group(1)))

            if len(block) > 50000:
                sqlQ.put(block)
                block = []
                
        except TypeError:
            logging.error(f'Regex {os.getpid()} failed on: {title}\n{text}')

        except:
            logging.error(f"Regex {os.getpid()} something else happened: {title}\n{text}")

    if len(block) > 0:
        sqlQ.put(block)
    sqlQ.put(None)
    logging.error(f"Regex {os.getpid()} finished")


def xml_worker(regQ):
    PATH_WIKI_XML = '/mnt/d/newest'
    FILENAME_WIKI = 'enwiki-latest-pages-articles-multistream.xml'
    PATH = os.path.join(PATH_WIKI_XML, FILENAME_WIKI)

    bar = progressbar.ProgressBar(max_value=progressbar.UnknownLength)
    i = 0
    
    logging.error(f"XML {os.getpid()} started")
    
    for _, elem in etree.iterparse(PATH):
        tname = strip_tag_name(elem.tag)

        if tname == 'title':
            title = elem.text
        elif tname == 'text':
            text = elem.text
            regQ.put((title, text))
            bar.update(i)
            i += 1
                    
        elem.clear()
        
    regQ.put(None)
    logging.error(f"XML {os.getpid()} finished")


if __name__ == '__main__':
    logging.basicConfig(filename="info.log", filemode='a', format='%(asctime)s %(message)s')
    logging.error(f"Main {os.getpid()} started")

    sqlQ = Queue(10000)
    regQ = Queue(10000)

    xmlP = Process(target=xml_worker, args=(regQ,))
    regP1 = Process(target=reg_worker, args=(regQ, sqlQ,))
    regP2 = Process(target=reg_worker, args=(regQ, sqlQ,))
    regP3 = Process(target=reg_worker, args=(regQ, sqlQ,))
    regP4 = Process(target=reg_worker, args=(regQ, sqlQ,))
    sqlP = Process(target=sql_worker, args=(sqlQ,))
    
    xmlP.start()
    regP1.start()
    regP2.start()
    regP3.start()
    regP4.start()
    sqlP.start()

    sqlQ.close()
    regQ.close()
    sqlQ.join_thread()
    regQ.join_thread()

    xmlP.join()
    regP1.join()
    regP2.join()
    regP3.join()
    regP4.join()
    sqlP.join()
    logging.error(f"Main {os.getpid()} finished")
    