
## Overview
This script is to make a description generation dataset from Wikipedia articles, and Wikidata descriptions. Note that running all of these scripts requires time (1~2 days) and consumes a large amount of memories due to the size of the original data.

## How to run
### 0. Preparations
 * Requirements (Other versions can be applicable, but not confirmed):
   - Python 3.6.0 
   - JDK 1.8
   - MySQL 5.7

 * Install python requirements: ```pip install -r requirements.txt```

 * Download Wikidata dump.
   ```
   mkdir -p wikidata/latest
   cd wikidata/latest
   wget https://dumps.wikimedia.org/wikidatawiki/entities/latest-truthy.nt.bz2
   bzip2 -d latest-truthy.nt.bz2
   cd ../../
   ```
 * Download Wikipedia dump.
   ```
   mkdir -p wikipedia/latest
   cd wikipedia/latest
   wget https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2
   bzip2 -d enwiki-latest-pages-articles.xml.bz2
   cd ../../
   ```

 * Download Wikipedia sql files (page.sql, redirect.sql, wbc_entity_usage.sql) and import them to MySQL database.
   ```
   cd wikipedia/latest
   wget https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-page.sql.gz
   wget https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-redirect.sql.gz
   wget https://dumps.wikimedia.org/enwiki/latest/enwiki-wbc_entity_usage.sql.gz
   gzip -d enwiki-latest-page.sql.gz
   gzip -d enwiki-latest-redirect.sql.gz
   gzip -d enwiki-latest-wbc_entity_usage.sql.gz
   # ***
   # Please google how to create a new database on MySQL and import sql files ...
   # ***
   cd ../../
   ```

### 1. Download Stanford-Corenlp (>= 3.8.0) and run it.
```
wget http://nlp.stanford.edu/software/stanford-corenlp-full-2018-10-05.zip 
unzip stanford-corenlp-full-2018-10-05.zip 
cd stanford-corenlp-full-2018-10-05
nohup java -mx10g -cp "*" edu.stanford.nlp.pipeline.StanfordCoreNLPServer -port 9000 -timeout 15000 -threads 10 >/dev/null 2>/dev/null
cd ..
```

### 2. Extract all items, properties, triples from Wikidata.
```
python wd_extract_all.py -s wikidata/latest/latest-truthy.nt -o wikidata/latest/extracted
```

### 3. Parse xml files to jsonlines by WikiExtractor.py.
```
python WikiExtractor.py wikipedia/latest/enwiki-latest-pages-articles.xml -o wikipedia/latest/extracted --filter_disambig_pages --json
# Note that this WikiExtractor.py has several modifications from the original one (https://github.com/attardi/wikiextractor).
```

### 4. Extract linked sentences from Wikipedia xml dump parsed by WikiExtractor.py.
```
python wp_extract_all.py DB_USERNAME DB_PASSWORD --dbname=DB_NAME -s wikidata/latest/latest-truthy.nt -o wikidata/latest/extracted

```

### 5. Merge Wikidata definitions and triples into the json file extracted from Wikipedia.
```
python wp_merge_defs_and_triples.py $PATH_TO_OUTPUT_DATASET
```


