from distutils.log import error
from multiprocessing import Queue, Process
import xml.etree.ElementTree as etree
import re
import os
import time
import sqlite3
import logging
import progressbar


def strip_tag_name(t):
    idx = t.rfind("}")
    if idx != -1:
        t = t[idx + 1:]
    return t


def startLog(pid, description):
    logging.info(f"pid:{pid} {description} started")


def endLog(pid, description):
    logging.info(f"pid:{pid} {description} finished")


def errorLog(pid, description, title=None, text=None):
    if title == None:
        logging.error(f'pid:{pid} {description}')
    else:
        logging.error(f'pid:{pid} {description}\n\tTitle: {title}\n\tText:  {text}')


def xml_worker(regQ):
    startLog(os.getpid(), "XML")

    bar = progressbar.ProgressBar(max_value=progressbar.UnknownLength)
    num_pages = 0
    for _, elem in etree.iterparse(PATH_WIKI_XML):
        tname = strip_tag_name(elem.tag)

        if tname == 'title':
            title = elem.text
        elif tname == 'text':
            text = elem.text
            regQ.put((title, text))
            bar.update(num_pages)
            num_pages += 1
                    
        elem.clear()
        
    for _ in range(NUM_REGEX_PROC):
        regQ.put(None)

    endLog(os.getpid(), "XML")


def reg_worker(regQ, sqlQ):
    startLog(os.getpid(), "Regex")

    block = []
    while True:
        try:
            title, text = regQ.get()

        except TypeError:
            errorLog(os.getpid(), "Regex - Found 'None'")
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
            errorLog(os.getpid(), "Regex - Failed", title, text)

        except:
            errorLog(os.getpid(), "Regex - Other", title, text)

    if len(block) > 0:
        sqlQ.put(block)
    time.sleep(10)
    sqlQ.put(None)

    endLog(os.getpid(), "Regex")


def sql_worker(q):
    startLog(os.getpid(), "SQL")

    con = sqlite3.connect(PATH_DB)
    cur = con.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS pages(page, link)")

    count = 0
    while True:
        block = q.get()

        if block is None:
            break

        cur.executemany("INSERT INTO pages VALUES(?, ?)", block)
        count += 1
        if count > 100:       
            con.commit()
            count = 0

    con.commit()
    con.close()
    endLog(os.getpid(), "SQL")   


if __name__ == '__main__':
    PATH_WIKI_XML = '/mnt/d/newest/enwiki-latest-pages-articles-multistream.xml'
    PATH_DB = '/mnt/d/wikilinks.db'
    REGEX = r'(?:\[\[)([^\[\]]+?)(?:\|[^\[\]]*)?(?:\]\])'

    NUM_REGEX_PROC = 4
    QUEUE_SIZE = 10000

    logging.basicConfig(
        filename="info.log", 
        filemode='a', 
        format='%(asctime)s %(message)s', 
        level=20)
    startLog(os.getpid(), "Main")

    sqlQ = Queue(QUEUE_SIZE)
    regQ = Queue(QUEUE_SIZE)

    xmlP = Process(target=xml_worker, args=(regQ,))
    regP = []
    for i in range(NUM_REGEX_PROC):
        p = Process(target=reg_worker, args=(regQ, sqlQ,))
        regP.append(p)
    sqlP = Process(target=sql_worker, args=(sqlQ,))
    
    xmlP.start()
    for p in regP:
        p.start()
    sqlP.start()

    sqlQ.close()
    sqlQ.join_thread()
    regQ.close()
    regQ.join_thread()

    xmlP.join()    
    for p in regP:
        p.join()
    sqlP.join()
    endLog(os.getpid(), "Main")
