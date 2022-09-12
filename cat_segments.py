from elasticsearch import Elasticsearch

import os
import logging

tsv_file = 'segments.tsv'

es = Elasticsearch(
    "http://" + os.getenv('ELASTICSEARCH_HOSTS') + ":9200",
    http_auth=(os.getenv('ELASTICSEARCH_USERNAME'), os.getenv('ELASTICSEARCH_PASSWORD'))
)

logging.basicConfig(format='%(message)s')
logging.getLogger().setLevel(logging.INFO)
logging.Formatter('%(asctime)s %(clientip)-15s %(user)-8s %(message)s')
logging.getLogger('elastic_transport.transport').setLevel(logging.WARN)
logger = logging.getLogger('cat_segments.py')

all_cols = []
for line in es.cat.segments(help=True).splitlines():
    parts = line.split('|')
    all_cols.append({
        "title": parts[0].strip(),
        "desc": parts[2].strip()
    })

harg = ''
for idx, col in enumerate(all_cols):
    if idx > 1:
        harg+=','
    harg+=col['title']

lines = es.cat.segments(h=harg, v=True).splitlines()

with open(tsv_file, 'w', encoding = 'utf-8') as f:
    tsv_lines = []
    for line in lines:
        for cell in line.split():
            f.write(cell)
            f.write('\t')
        f.write('\n')
