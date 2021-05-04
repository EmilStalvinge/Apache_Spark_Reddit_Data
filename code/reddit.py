#!/usr/bin/env python3

# A reddit comments file downloader and preprocessor
#
# Usage: ./reddit.py <YYYY-MM>

import sys, json, bz2, urllib.request
from datetime import datetime

yearmonth = sys.argv[1]
site = 'http://files.pushshift.io/reddit/comments/'
sitefilename = 'RC_' + yearmonth + '.bz2'
outfilename = 'rc-' + yearmonth + '.csv'
decompressor = bz2.BZ2Decompressor()
blocksize = 4*1024*1024
rest = ''

print('Downloading and preprocessing reddit comments file ' + sitefilename)
print('Working...', end='', flush=True)
with urllib.request.urlopen(site + sitefilename) as sitefile, open(outfilename, 'w') as outfile:
    while block := sitefile.read(blocksize):
        data = decompressor.decompress(block)
        lines = data.decode('utf-8').splitlines()
        for line in lines:
            try:
                d = json.loads(line)
            except json.decoder.JSONDecodeError:
                if rest == '':
                    rest = line
                    continue
                else:
                    d = json.loads(rest + line)
                    rest = ''
            ts = int(d['created_utc'])
            subreddit = d['subreddit']
            date = datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d')
            outfile.write(f'{date}, {subreddit}\n')
        print('.', end='', flush=True)
print('\nDone!')
