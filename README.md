
### 0. Preparation
 * Install python 3.6.0. 
 * Download Wikidata dump file.
 * Download Wikipedia xml and sql files (ages-articles.xml, page.sql, redirect.sql, wbc_entity_usage.sql) and import them to MySQL database.
 * Download Corenlp to $corenlp_dir and setup java 1.8.

 * run below.
 '''
 pip install -r requirements.txt
 '''

### 1. Extract all items, properties, triples from Wikidata.
```
python wd.extract_all.py $target_dir
```

### 2. Tokenize the entities' description, aka.
```
python wd.tokenize.py
```

### 3. Parse xml files (enwiki-***-pages-articles.xml) to text by WikiExtractor.py with several modification from the original script.
python WikiExtractor wikipedia/latest/xml/enwiki-*-pages-articles.xml -o $target_dir --filter_disambig_pages --json'

### 4. Extract linked sentences from Wikipedia xml dump parsed by WikiExtractor.py.
```
cd $corenlp_dir
java -mx8g -cp "*" edu.stanford.nlp.pipeline.StanfordCoreNLPServer -port 9000 -timeout 15000 
cd ..
python wp.extract_all.py db_username dbpass
```

### 5. Merge Wikidata definitions and triples into the json file extracted from Wikipedia.
```
python wp_merge_defs_and_triples.py $target_dir
```


