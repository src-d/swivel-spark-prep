#!/usr/bin/env python

from pprint import pprint
from collections import defaultdict

vocab = {'a': 1, 'b': 2, 'c': 3 }

def words(line):
  return line

def cooc(lines):
  window_size = 10
  sums = defaultdict(int)
  coocs_dict = {}
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
        coocs_dict.setdefault(pair, 0.0)
        coocs_dict[pair] += count

      sums[lid] += 1.0
      pair = (lid, lid)
      coocs_dict.setdefault(pair, 0.0)
      coocs_dict[pair] += 0.5  # Only add 1/2 since we output (a, b) and (b, a)

  coocs = [] # for (a, b) add (b, a)
  for pair, count in coocs_dict.iteritems():
    coocs.append((pair[0], pair[1], count))
    coocs.append((pair[1], pair[0], count))

  # Sort and merge co-occurrences for the same pairs.
  coocs.sort()

  if coocs:
    current_pos = 0
    current_row_col = (coocs[current_pos][0], coocs[current_pos][1])
    for next_pos in range(1, len(coocs)):
      next_row_col = (coocs[next_pos][0], coocs[next_pos][1])
      if current_row_col == next_row_col:
        coocs[current_pos] = (
            coocs[current_pos][0],
            coocs[current_pos][1],
            coocs[current_pos][2] + coocs[next_pos][2])
      else:
        current_pos += 1
        if current_pos < next_pos:
          coocs[current_pos] = coocs[next_pos]

        current_row_col = (coocs[current_pos][0], coocs[current_pos][1])

    coocs = coocs[:(1 + current_pos)]

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
