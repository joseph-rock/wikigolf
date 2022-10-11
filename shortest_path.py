import sqlite3

def get_links(page_name):
    CURSOR.execute("SELECT link FROM pages WHERE page = ?", (page_name,))
    return CURSOR.fetchall()


def start(source, target):
    visited = set()
    queue = []

    visited.add(source)
    queue.append(source)
    i = 0

    while queue:
        m = queue.pop(0)
        print(m)

        if m == target:
            return i
        
        i+=1

        for link in get_links(m):
            if link not in visited:
                visited.add(link)
                queue.append(link)

    return -1

if __name__ == '__main__':
    PATH_DB = '/mnt/d/db/wikilinks.db'
    CONNECTION = sqlite3.connect(PATH_DB)
    CONNECTION.row_factory = lambda cursor, row: row[0]
    CURSOR = CONNECTION.cursor()

    START = "Djent"
    END = "Katy Perry"

    print(start(START, END))