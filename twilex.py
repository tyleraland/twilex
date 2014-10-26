#!/usr/bin/env python

activate_this = '/Users/tal/sandbox/bin/activate_this.py'
execfile(activate_this, dict(__file__=activate_this))

import sys
import argparse
import sqlite3
from datetime import datetime, timedelta
from pytz import timezone
from tzwhere.tzwhere import tzwhere

from string import letters
from itertools import imap, chain, groupby

namer = tzwhere()

def parse_timestamp(user_timestamp, sms_timestamp):
    # Given: timestamp and a user-given time annotation for the record
    # Result: A string of the datetime the user was referring to
    #
    # user_timestamp begins with a '$' and is followed by one or two incompletely
    # described datetime strings; omitted values are inferred using sms_timestamp
    # At the bare minimum, an hour and a minute must be provided.  Hour/minutes are
    # not delimited, so they must use the surrounding context.
    # E.g., $815 is valid and means 8:15
    # Furthermore, they are on a 24-hour clock
    # E.g., $1315 is 1:15 pm
    # Times may be preceded by an incompletely described date string and a 'T':
    # 01T1234 : First day of month at 12:34
    # 0901T1234 : First day of September at 12:34
    # 20140901T1234 : First day of September 2014 at 12:34
    # The elements of a complete datetimestring are year, month, day, and time
    # Note that the above year/month/day format uses fixed column widths.  
    # Finally, datetimestring as described above can come in pairs, which are 
    # separated by a dot.
    # E.g., 20140901T1000.20140902T1100 describes two times, perhaps start.stop
    # 
    # To complete a user_timestamp, we begin with the largest provided unit; based
    # on the next highest unit in sms_timestamp, we decide if the time would be in the
    # future.  If so, we decrement that higher unit value in sms_timestamp.
    # E.g., user_timestamp = $2200, sms_timestamp = 20140901T900
    # We know the timestamp was sent at 9:00am.  However the user_timestamp refers 
    # to 10:00pm.  So we infer the user meant last night: 20140831T2200
    assert(user_timestamp[0] == '$')
    times = user_timestamp[1:].split('.') # Either $time or $time1.time2
    sms_datetime = datetime.strptime(sms_timestamp, "%Y-%m-%dT%H:%M:%S")
    outstrings = []
    for time in times:
        assert(len(time) >= 3) # Last 3-4 digits are hour and minute
        minute = int(time[-2:])
        if 'T' not in time:
            # We must check sms_timestamp time and decide the day
            hour = int(time[:-2])
            if (hour > sms_datetime.hour or
                hour == sms_datetime.hour and minute > sms_datetime.minute):
                yesterday = sms_datetime - timedelta(days=1)
                day = yesterday.day
                month = yesterday.month
                year = yesterday.year
            else:
                day = sms_datetime.day
                month = sms_datetime.month
                year = sms_datetime.year
        else:
            # 'T' was found in time.  Therefore it is preceded by [[[Year]Month]Day]
            hour = int(time.split('T')[1][:-2]) # From T to the final two numbers
            ymd = time.split('T')[0]
            assert(ymd.isdigit() and len(ymd) == 8)
            day = int(ymd[-2:])
            month = int(ymd[-4:-2])
            year = int(ymd[-8:-4])
        dt = datetime(year, month, day, hour, minute)
        outstrings.append(dt.strftime("%Y-%m-%dT%H:%M:%S"))
    if len(outstrings) == 2:
        return outstrings
    else:
        return outstrings + ['']

def parse_group(sets_at_magnitude):
    # Given: group of "sets" of units (period-separated string) with a leading magnitude
    # E.g. sets_at_magnitude = 45.5.5.4 is two sets of five counts at 45, and another 
    # set of four counts at 45
    # Returns a list of each set with its magnitude, e.g. [(45,5),(45,5),(45,4)]
    magnitude = sets_at_magnitude.split('.')[0]
    sets = sets_at_magnitude.split('.')[1:]
    if len(sets) == 0:
        # measureset of 100, is implicitly 100.1, one set of one count of 100
        return [(magnitude,'1')]
    else:
        return [(magnitude,count) for count in sets]

def parse_entry(entry):
    # Unit, Set, Magnitude
    # Each entry begins with a shortname, followed by a variable number of groups of 
    # sets of units.  Each group of sets begins with a magnitude (integer) and is 
    # followed by 0 or more sets of units, separated by periods.  If no sets follow 
    # a magnitude, it's  assumed to be one set of one count at that magnitude.
    # So an entry may be: "sq 45.5 65.5.5.4 120"
    # In the above example, there are is one set of five units of 45, 
    # two sets of five units of 65, one set of four units of 65, and one set of 
    # one count at 120.
    # To capture all this we say magnitudes = 45,65,65,120
    # Those magnitudes corresponding sets   =  1, 2, 1, 1
    # Those sets corresponding units        =  5, 4, 5, 1

    shortname = entry[0]
    
    # Chops each group of sets at the same magnitude into a list of singleton groups, 
    # each with one set and a magnitude.  
    measurements = list(chain.from_iterable(imap(parse_group, entry[1:])))

    # Sets are absorbed into new groups of the same magnitude and count
    quantities = [(k[0],
                   k[1],
                   str(len(list(g))))
                   for k,g in groupby(sorted(measurements))]

    # Finally, we want the magnitude, sets, and counts for each of these groups
    magnitudes = ','.join([q[0] for q in quantities])
    counts     = ','.join([q[1] for q in quantities])
    sets       = ','.join([q[2] for q in quantities])

    return [shortname,
            magnitudes,
            sets,
            counts]

def parse_tweet(tweet):
    datetimestring = tweet[0]
    text = tweet[1]
    # Leading 'r' indicates a record
    if text[0].lower() != 'r':
        return iter([])

    words = text.split()[1:] # Ignore the first 'r'

    timestamp = [w for w in words if w[0] == '$']
    if timestamp:
        assert(len(timestamp) == 1)
        dt1, dt2 = parse_timestamp(timestamp[0], datetimestring)
        words.remove(timestamp[0])
    else:
        dt1, dt2 = datetimestring, ''

    # Each item entry is alphabetical and followed by groups of 
    # period-separated-integers separated by spaces
    items = [(pos, word) for pos,word in enumerate(words) if word[0] in letters]
    # Grabs lower:upper indices of of words belonging to each item in words
    indices = zip([i[0] for i in items],
                  [i[0] for i in items[1:]] + [1+len(words)])
    entries = [(words[l:h]) for l,h in indices]
    
    # Prepend each list of tokens with the datetime it was logged for
    return ([dt1, dt2] + entry for entry in imap(parse_entry,
                                                       entries))

def localize(tweet):
    dt = datetime.strptime('T'.join([tweet[0],tweet[1]]), "%Y-%m-%dT%H:%M:%S")
    utc = timezone('UTC')
    dt = utc.localize(dt)
    # Use lat, long to determine timezone name
    localtzname = namer.tzNameAt(tweet[2],tweet[3])
    localtz = timezone(localtzname)
    dt = dt.astimezone(localtz)
    return [dt.strftime("%Y-%m-%dT%H:%M:%S"),
            tweet[4]]

def fetch_rows(database, twitter_table, gps_table):
    con = sqlite3.connect(database)
    cur = con.cursor()

    for table_name in [twitter_table.lower(), gps_table.lower()]:
        for bad in ['select','drop','from','where','insert',' ']:
            assert(bad not in table_name)
    # Statement to grab tweets and associate with each one the GPS coordinate 
    # pair temporally closest to each tweet
    statement = """
    select substr(datetime, 0, 11) as t_date, 
           substr(datetime, 12, 8) as t_time,
           latitude, longitude, 
           message, 

           abs((360 * cast(substr(g_time, 1, 2) as decimal) +
                60  * cast(substr(g_time, 4, 2) as decimal) +
                      cast(substr(g_time, 7, 2) as decimal)) -
               (360 * cast(substr(substr(datetime, 12, 8), 1, 2) as decimal) +
                60  * cast(substr(substr(datetime, 12, 8), 4, 2) as decimal) +
                      cast(substr(substr(datetime, 12, 8), 7, 2) as decimal)
           )) as seconds_away
    from {}
     join (
       select substr(datetime, 0, 11) as g_date, 
              substr(datetime, 12, 8) as g_time, 
              latitude, 
              longitude 
       from {}
       group by substr(g_time, 0, 3)
     ) 
    where t_date == g_date
    group by t_date, t_time, message
    order by t_date asc, t_time asc, seconds_away asc
    """.format(twitter_table, gps_table)
    return cur.execute(statement).fetchall()

def dbcreate(database):
    con = sqlite3.connect(database)
    cur = con.cursor()
    statements = [
    """
    CREATE table if not exists
    USER_ENTRIES( shortname TEXT,
                  datetime1 TEXT,
                  datetime2 TEXT,
                  magnitude TEXT,
                  sets TEXT,
                  counts TEXT,
                  UNIQUE(shortname,datetime1,datetime2)
                on conflict replace);
    """,
    """
    CREATE table if not exists
    ITEM_INFO( shortname TEXT,
               linked_table TEXT, -- E.g. Food
               table_id TEXT,     -- Index into linked_table
               multiplier REAL,   -- 
               units TEXT,        -- How is it being measured?
               UNIQUE(shortname)
             on conflict replace)

    """]
    for statement in statements:
        cur.execute(statement)
    con.commit()

def dbinsert(database, table, rows):
    con = sqlite3.connect(database)
    cur = con.cursor()

    con.text_factory = str
    for row in rows:
        statement = "insert or replace into {} values ({})".format(
            table, ','.join(['?' for field in row])
        )
        cur.execute(statement, row)
    con.commit()

def get_args():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--twitter_gps', nargs=3, 
                        default=['/Users/tal/data/feeds.db','twitter','gps'],
                        help="Database path and table names containing twitter and gps data respectively")
    parser.add_argument('--target', nargs=2,
                        default=['/Users/tal/data/selftracking.db','manual_entries'],
                        help="Database and table to insert parsed entries")
    return parser.parse_args()                   
def main():
    args = get_args()

    dbcreate(args.target[0])
    # fetch_rows grabs tweet,gps rows from database
    # Next we shift that tweet's timestamp from UTC to its local time
    # parse each row into 0 or many entries
    # Insert all entries into the database
    for row in fetch_rows(*args.twitter_gps):
        tweet = localize(row)
        parsed = parse_tweet(tweet)
        dbinsert('tracking.db', 'user_entries', parsed)

if __name__ == '__main__':
    sys.exit(main())
