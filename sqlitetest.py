import sqlite3

con = sqlite3.connect("wikilinks.db")
cur = con.cursor()
cur.execute("CREATE TABLE IF NOT EXISTS pages(source, target)")