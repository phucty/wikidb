WikiDB: Build a DB (key-value store - LMDB style) from Wikidata dump
---

Build a local WikiDB from Wikidata dumps. We can fast access Wikidata item information, fact provenances, search and filter wikidata (known their attribute value (Wikidata ID)). 

Minimum requirements: 
- SSD: ~300 GB
- Ram: 4GB

### 1. Config your setting
Modify [config.py](config.py) to your setting

- **DIR_ROOT** to your project director, e,g,. `/Users/phucnguyen/git/wikidb`
- Select **DUMPS_WD_JSON** [version](https://dumps.wikimedia.org/wikidatawiki/entities/) (e.g., `20220131`) and **DUMPS_WD_SQL** [version](https://dumps.wikimedia.org/wikidatawiki/) (e.g., `20220201`).

### 2. Create venv and install dependencies
``` 
conda create -n wikidb python=3.6
conda activate wikidb
pip install -r requirements.txt
``` 

### 3. Download Wikidata dumps
``` 
python download_dump.py
```
`Run time: 4 hours`

We will download the three files:
- `wikidata-{JSON_VER}-all.json.bz2`: All content of Wikidata items
- `wikidatawiki-{SQL_VER}-page.sql.gz`: Get local ID of Wikidata, and build Wikidata ID trie
- `wikidatawiki-{SQL_VER}-redirect.sql.gz`: Get redirect Wikidata items

### 4. Build wikidb
``` 
python build_db.py
```
- This will first parse `wikidatawiki-{SQL_VER}-page.sql.gz` and build trie mapping from Wikidata ID item to local database ID (int), e.g., Q31 (str): 2 (int).
- Extract redirects from `wikidatawiki-{SQL_VER}-redirect.sql.gz`
- Parse `wikidata-{JSON_VER}-all.json.bz2` and save to db

`Run time: 1 or 2 days`

### 5. Use wikidb
Refer to [example.py](example.py)

``` python
from core.db_wd import DBWikidata

db = DBWikidata()

# Get label in english
print(db.get_label("Q31"))

# Gel label in all languages
print(db.get_labels("Q31"))

# Gel aliases in all languages
print(db.get_aliases("Q31"))

# Gel descriptions in all languages
print(db.get_descriptions("Q31"))

# Gel descriptions in all languages
print(db.get_descriptions("Q31"))

# Gel sitelinks
print(db.get_sitelinks("Q31"))

# Gel claims
print(db.get_claims("Q31"))

# Get all information about Q31
print(db.get_item("Q31"))

# Get redirect of Q31
redirects = db.get_redirect_of("Q31")
print(redirects)

# Get redirect
print(db.get_redirect(redirects[0]))

# Get instance of Q31
print(db.get_instance_of("Q31"))

# Get subclass of Q31
print(db.get_subclass_of("Q31"))

# Get all types of Q31
print(db.get_all_types("Q31"))

``` 

### 6. Todo
- Prepare downloadable indexed model link
- Boolean search similar to haswbstatement of Cirrus Search of Wikidata
- Provenance information extraction


### LICENSE
wikidb code is licensed under MIT License.

The LMDB lib is under OpenLDAP Public License (permissive software license)

Python binding: https://github.com/jnwatson/py-lmdb/blob/master/LICENSE

Original LMDB: https://github.com/LMDB/lmdb/blob/mdb.master/libraries/liblmdb/LICENSE
