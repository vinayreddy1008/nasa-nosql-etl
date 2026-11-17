#!/usr/bin/env python3
"""Second MapReduce pass: forward only Q2 rows for GLOBAL top-20."""
import sys
for line in sys.stdin:
    if line.startswith('Q2\t'):
        print('GLOBAL\t'+line.strip())
