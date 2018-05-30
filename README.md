
### 0. Preparation
 * Download Wikidata/Wikipedia dumps.  
 * Import Wikipedia *.sql to MySQL database.
 * Prepare Stanford Tokenizer and Corenlp.
 * Prepare WikiExtractor.

### 1. Extract all items, properties, triples from Wikidata.
```
python wd.extract_all.py target_dir
```

### 2. Tokenize the entities' description, aka.
```
python wd.tokenize.py
```

### 3. Extract linked sentences from Wikipedia xml dump parsed by WikiExtractor.py.
```
python wp.extract_all.py
```

### 4. Merge Wikidata definitions and triples into the json file extracted from Wikipedia.
```
python wp_merge_defs_and_triples.py target_dir
```

