import xml.etree.ElementTree as etree
import re
import os

PATH_WIKI_XML = '/mnt/d/newest'
FILENAME_WIKI = 'enwiki-latest-pages-articles-multistream.xml'

pathWikiXML = os.path.join(PATH_WIKI_XML, FILENAME_WIKI)

def strip_tag_name(t):
    idx = t.rfind("}")
    if idx != -1:
        t = t[idx + 1:]
    return t

count = 0
redirect = False
for event, elem in etree.iterparse(pathWikiXML, events=('start', 'end')):
    tname = strip_tag_name(elem.tag)

    if event == 'start':
        if tname == "title":
            t = elem.text
        if tname == "redirect":
            redirect = True

    elif event == 'end':
        if tname == 'text' and redirect:
            redirect = False
            continue
        elif tname == 'text' and not redirect:
            l = re.findall('(?:\[{2})([\w\s]+)(?:[|\w\s]*)(?:\]{2})', elem.text)
            print(f"{count})--{t} \n {l}")

    #     t = elem.text
    #     print(t)
    # elif tname == "text":
    #     l = re.findall("\[\[.+\]\]", elem.text)
    #     print(f"{t} : {l}")

    # count += 1
    # if count == 200:
    #     break