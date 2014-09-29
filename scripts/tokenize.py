#!/usr/bin/env python

from collections import OrderedDict
from string import letters
from itertools import imap, chain, groupby

def parse_timestamp(timestamp):
    return None

def parse_group(sets_at_magnitude):
    # Given: a group of "sets" of units (period-separated string) with a a single leading magnitude
    # E.g. sets_at_magnitude = 45.5.5.4 is two sets of five units at 45, and another four at 45
    # Returns a list of each set with its magnitude, e.g. [(45,5),(45,5),(45,4)]
    magnitude = sets_at_magnitude.split('.')[0]
    sets = sets_at_magnitude.split('.')[1:]
    if len(sets) == 1:
        # if measureset == 100, then this is implicitly 100.1, one set of one count of 100
        return [(magnitude,'1')]
    else:
        return [(magnitude,count) for count in sets]

def parse_entry(entry):
    # Unit, Set, Magnitude
    # Each entry begins with a shortname, followed by a variable number of groups of sets of 
    # units.  Each group of sets begins with a magnitude (integer) and is followed by
    # 0 or more sets of units, separated by periods.  If no sets follow a magnitude, it's 
    # assumed to be one set of one count at that magnitude.
    # So an entry may be: "sq 45.5 65.5.5.4 120"
    # In the above example, there are is one set of five units of 45, two sets of five units
    # of 65, one set of four units of 65, and one set of one count at 120
    # To capture all this we say magnitudes   : 45,65,65,120
    # Those magnitudes corresponding sets are :  1, 2, 1, 1
    # Those sets corresponding units are      :  5, 4, 5, 1

    shortname = entry[0]
    
    # Chops each group of sets at the same magnitude into a list of singleton groups, each
    # with one set and a magnitude.  
    measurements = list(chain.from_iterable(imap(parse_group, entry[1:])))

    # Sets are absorbed into new groups of the same magnitude and count
    quantities = [(k[0],
                   k[1],
                   str(len(list(g))))
                   for k,g in groupby(sorted(measurements))]
    # Finally, we want the magnitude, sets, and counts for each of these groups
    magnitude = ','.join([q[0] for q in quantities])
    count     = ','.join([q[1] for q in quantities])
    sets      = ','.join([q[2] for q in quantities])

    return [shortname,
            magnitude,
            sets,
            count]

def parse_message(message):
    dt = message[0]
    text = message[1]
    # Leading 'r' indicates a record
    if text[0].lower() != 'r':
        return None

    words = text.split()[1:] # Now ignore the first 'r'

    timestamp = [w for w in words if w[0] == '$']
    if timestamp:
        assert(len(timestamp) == 1)
        dt = parse_timestamp(timestamp[0])
        words.remove(timestamp[0])

    # Each item entry is alphabetical and followed by groups of period-separated-integers separated
    # by spaces
    items = [(pos, word) for pos,word in enumerate(words) if word[0] in letters]
    # Grabs lower:upper indices of of words belonging to each item in words
    indices = zip([i[0] for i in items],
                  [i[0] for i in items[1:]] + [1+len(words)])
    entries = [(words[l:h]) for l,h in indices]
    
    parsed = imap(parse_entry, entries)
    
    return imap(lambda entry: entry[0:1] + [dt] + entry[1:],
                parsed)

# Takes in rows of messages, returns rows of records
def tokenize(rows):
    #return chain(imap(records, rows))
    return parse_message(rows[0])
