#!/usr/bin/env python3
"""Hadoop Streaming reducer: implements Q1, Q2 first pass, Q3 and metadata."""
import sys
SEP='\x1f'


def flush(key, vals):
    if key is None:return
    parts=key.split(SEP)
    tag=parts[0]
    if tag=='M':
        print('M\tBAD\t'+str(sum(int(v) for v in vals)))
    elif tag=='B':
        print('B\t'+parts[1]+'\t'+parts[2]+'\t'+str(sum(int(v) for v in vals)))
    elif tag=='Q1':
        print('Q1\t'+parts[1]+'\t'+parts[2]+'\t'+str(len(vals))+'\t'+str(sum(map(int,vals))))
    elif tag=='Q2':
        total=0; hosts=set()
        for v in vals:
            amount,host=v.split(SEP,1)
            total+=int(amount);hosts.add(host)
        print('Q2\t'+parts[1]+'\t'+str(len(vals))+'\t'+str(total)+'\t'+str(len(hosts)))
    elif tag=='Q3':
        errors=0; hosts=set()
        for v in vals:
            flag,host=v.split(SEP,1)
            if flag=='1':
                errors+=1;hosts.add(host)
        total=len(vals)
        print('Q3\t'+parts[1]+'\t'+parts[2]+'\t'+str(errors)+'\t'+str(total)+
              '\t'+str(errors/total)+'\t'+str(len(hosts)))
    else:raise RuntimeError('Unknown map key: '+key)


def main():
    prev=None; values=[]
    for line in sys.stdin:
        key,val=line.rstrip('\n').split('\t',1)
        if prev is not None and key!=prev:
            flush(prev,values)
            values=[]
        prev=key
        values.append(val)
    flush(prev,values)

if __name__=='__main__':main()
