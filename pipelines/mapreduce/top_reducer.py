#!/usr/bin/env python3
"""Second MapReduce pass, reduce all resource paths to global top 20."""
import heapq
import sys
rows=[]
for line in sys.stdin:
    _, datum=line.rstrip('\n').split('\t',1)
    tag,path,n,total,hosts=datum.split('\t',4)
    assert tag=='Q2'
    rows.append((path,int(n),int(total),int(hosts)))
for path,n,total,hosts in sorted(rows, key=lambda r:(-r[1],r[0]))[:20]:
    print(f'Q2\t{path}\t{n}\t{total}\t{hosts}')
