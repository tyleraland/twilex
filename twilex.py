#!/usr/bin/env python

import sys
from scripts.tokenize import tokenize
import sqlite3

def main():
    con = sqlite3.connect('../etcetera/feeds.db')
    cur = con.cursor()
    rows = cur.execute("select * from twitter order by datetime desc limit 1").fetchall()
    #ret = tokenize(rows)
    ret = tokenize([('datetime', 'R $2014-09-27T2230.2014-09-28T120 sq 45.5.5 75.5.5.4 65.4')])
    #ret = tokenize([('datetime', 'R $2014-09-27T2230.2014-09-28T120 avocado 45 bacon 10')])
    for r in ret:
        print(r)
if __name__ == '__main__':
    sys.exit(main())
