# -*- coding: utf-8 -*-
#!/usr/bin/env python
import sys
import csv

first_line = True
for line in sys.stdin:
    if first_line:
        first_line = False
        continue  # sauter l'en-tête
    row = list(csv.reader([line]))[0]
    try:
        age, job, marital, housing, loan, y = int(row[0]), row[1], row[2], row[5], row[6], int(row[12])
    except:
        continue  # ignorer les lignes mal formées
    score = 0
    if 25 <= age <= 40: score += 1
    if job in ['0','1','2','3','4','5','6','7','8','9','10']: score += 1  # exemple simplifié
    if housing == '1': score += 1
    if loan == '0': score += 1
    if marital in ['0','1']: score += 0.5
    print("%s\t%d" % (score, y))

