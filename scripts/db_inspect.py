import os, sqlite3, json

db_path = os.path.join('src','db','hymns_OK.sqlite3')
print('DB exists:', os.path.exists(db_path))
con = sqlite3.connect(db_path)
cur = con.cursor()
print('Tables:')
for name, sql in cur.execute("SELECT name, sql FROM sqlite_master WHERE type='table'"):
    print(name)
    print(sql)
print('Views:')
for (name,) in cur.execute("SELECT name FROM sqlite_master WHERE type='view'"):
    print(name)
cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name LIMIT 1")
row = cur.fetchone()
if row:
    t = row[0]
    print('Sample of table:', t)
    print('Columns:', list(cur.execute(f"PRAGMA table_info({t})")))
    print('Rows:', list(cur.execute(f"SELECT * FROM {t} LIMIT 3")))
