# -*- coding: utf-8 -*-
#!/usr/bin/env python
import sys
from collections import defaultdict

counts = defaultdict(lambda: [0,0])  # score -> [total_clients, total_yes]

for line in sys.stdin:
    score, yes = line.strip().split('\t')
    score = float(score)
    yes = int(yes)
    counts[score][0] += 1
    counts[score][1] += yes

for score in sorted(counts.keys()):
    total, yes_count = counts[score]
    rate = float(yes_count) / total
    print("%s\t%d\t%d\t%.2f" % (score, total, yes_count, rate))

