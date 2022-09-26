from elasticsearch import Elasticsearch

import os
import logging

tsv_file = 'nodes.tsv'

es = Elasticsearch(
    "http://" + os.getenv('ELASTICSEARCH_HOSTS') + ":9200",
    http_auth=(os.getenv('ELASTICSEARCH_USERNAME'), os.getenv('ELASTICSEARCH_PASSWORD'))
)

logging.basicConfig(format='%(message)s')
logging.getLogger().setLevel(logging.INFO)
logging.Formatter('%(asctime)s %(clientip)-15s %(user)-8s %(message)s')
logging.getLogger('elastic_transport.transport').setLevel(logging.WARN)
logger = logging.getLogger('cat_nodes.py')

all_cols = []
for line in es.cat.nodes(help=True).splitlines():
    parts = line.split('|')
    all_cols.append({
        "title": parts[0].strip(),
        "desc": parts[2].strip()
    })

harg = ''
for idx, col in enumerate(all_cols):
    if idx > 0:
        harg+=','
    harg+=col['title']

# time option should be specified but it is not yet accepted. See elasticsearch-py#2066
# Ditto for include_unloaded_segments
lines = es.cat.nodes(h=harg, v=True, bytes='mb').splitlines()

line0 = lines[0]
last_char = 'x'
seps = []
for index in range(0, len(line0)):
    if last_char != ' ' and line0[index] == ' ': # first ' ' after title
        sep = index
        for line in lines[1:]:
            for index2 in range(sep, len(line)):
                if line[index2] == ' ': # first ' ' after value for this column
                    if index2 > sep:
                        sep = index2
                    break
        seps.append(sep)
    last_char = line0[index]

with open(tsv_file, 'w', encoding = 'utf-8') as f:
    tsv_lines = []
    for line in lines:
        for index in range(0, len(seps)):
            if index == 0:
                val = line[:seps[index]]
                f.write(val.strip())
                f.write('\t')
            elif index == len(seps) - 1:
                val = line[seps[index]:]
                f.write(val.strip())
                f.write('\n')
            else:
                val = line[seps[index-1]:seps[index]]
                f.write(val.strip())
                f.write('\t')
