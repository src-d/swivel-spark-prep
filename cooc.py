#!/usr/bin/env python

from pprint import pprint
from collections import defaultdict

vocab = {'a': 1, 'b': 2, 'c': 3 }

def words(line):
  return line

def cooc(lines):
  window_size = 10
  sums = defaultdict(int)
  coocs = {}
  for lineno, line in enumerate(lines, start=1):
    wids = filter(
      lambda wid: wid is not None,
      (w for w in words(line)))
    for pos in xrange(len(wids)):
      lid = wids[pos]
      window_extent = min(window_size + 1, len(wids) - pos)
      for off in xrange(1, window_extent):
        rid = wids[pos + off]
        #pair = (min(lid, rid), max(lid, rid))
        pair = (lid, rid)
        count = 1.0 / off
        sums[lid] += count
        sums[rid] += count
        coocs.setdefault(pair, 0.0)
        coocs[pair] += count

      sums[lid] += 1.0
      pair = (lid, lid)
      coocs.setdefault(pair, 0.0)
      coocs[pair] += 0.5  # Only add 1/2 since we output (a, b) and (b, a)
  
  print("Input")
  pprint(lines)
  print("Co-occurances")
  pprint(coocs)
  #print("Summs")
  #pprint(sums)
  #print("")
  return coocs


def main():
  cooc(lines=[["a", "b"]])

  #import pdb; pdb.set_trace()s
  cooc(lines=[["a", "b"],
              ["a", "b", "c"]])

  cooc(lines=[["a", "b"],
              ["a", "b", "c"],
              ["a", "b", "c", "d"]])

if __name__ == '__main__':
  main()
