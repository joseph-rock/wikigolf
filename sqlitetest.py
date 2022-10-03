import sqlite3

con = sqlite3.connect("/mnt/d/wikilinks.db")
cur = con.cursor()
# cur.execute("CREATE TABLE IF NOT EXISTS pages(page, link)")

res = cur.execute("SELECT * FROM pages")
print(res.fetchall())