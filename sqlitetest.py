import sqlite3

con = sqlite3.connect("wikilinks.db")
cur = con.cursor()
# cur.execute("CREATE TABLE IF NOT EXISTS pages(page, link)")

res = cur.execute("SELECT * FROM pages WHERE link = 'Joseph Raz'")
print(res.fetchall())