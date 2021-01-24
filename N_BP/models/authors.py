from __future__ import print_function

import io
import csv
import aerospike
import time

if __name__ == '__main__':
    config = {
        'hosts': [
            ('127.0.0.1', 3000)
        ],
        'policies': {
            'timeout': 1000  # milliseconds
        }
    }
    client = aerospike.client(config)
    client.connect()

    with io.open(r"models\nbdata\authors.csv", 'r', encoding="utf8",
             newline='') as authors:
        reader_authors = csv.reader(authors, delimiter=",", quotechar='"', quoting=csv.QUOTE_ALL)
        start_time = time.time()
        for author in reader_authors:
            current_author = {
                "name": author[1],
                "meibi": author[2],
                "meibix": author[3],
                "avg_words": author[4],
                "avg_words_no_stopwords": author[5],
                "posts": []
            }
            key = ('test', 'nbp', 'author_'+author[0])
            client.put(key, current_author)

        #author_json = json.dumps(current_author)
        #print(client.add("author_" + author[0], author_json))