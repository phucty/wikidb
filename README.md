WikiDB: Build a DB (key-value store - LMDB style) from Wikidata dump
---

Build a local WikiDB from Wikidata dumps. We can fast access Wikidata item information, fact provenances, search and filter wikidata (known their attribute value (Wikidata ID)). 

Minimum requirements: 
- DISK: ~300 GB

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
`Run time: 6 hours`

We will download the three files:
- `wikidata-{JSON_VER}-all.json.gz`: All content of Wikidata items
- `wikidatawiki-{SQL_VER}-page.sql.gz`: Get local ID of Wikidata, and build Wikidata ID trie
- `wikidatawiki-{SQL_VER}-redirect.sql.gz`: Get redirect Wikidata items

### 4. Build wikidb
``` 
python build_db.py
```
- This will first parse `wikidatawiki-{SQL_VER}-page.sql.gz` and build trie mapping from Wikidata ID item to local database ID (int), e.g., Q31 (str): 2 (int).
- Extract redirects from `wikidatawiki-{SQL_VER}-redirect.sql.gz`
- Parse `wikidata-{JSON_VER}-all.json.gz` and save to db

`Run time: 1 or 2 days`

### 5. Use wikidb
Refer to [example.py](example.py) or [example.ipynb](example.ipynb)

``` python
# Import class
from core.db_wd import DBWikidata
import config as cf

# Wikidb
db = DBWikidata(cf.DIR_WIKIDATA_ITEMS_JSON + "2")

# Get label of Belgium (Q31)
print(db.get_label("Q31"))

# Gel label in all languages of Belgium (Q31)
print(db.get_labels("Q31"))
# Get label in a specific language
print(db.get_labels("Q31", "ja"))

# Gel aliases in all languages of Belgium (Q31)
print(db.get_aliases("Q31"))
# Get aliases in a specific language of Belgium (Q31)
print(db.get_aliases("Q31", "ja"))

# Gel descriptions in all languages of Belgium (Q31)
print(db.get_descriptions("Q31"))
# Get descriptions in a specific language of Belgium (Q31)
print(db.get_descriptions("Q31", "ja"))

# Gel sitelinks of Belgium (Q31)
print(db.get_sitelinks("Q31"))

# Gel Wikipedia title of Belgium (Q31)
print(db.get_wikipedia_title("ja", "Q31"))
# Gel Wikipedia link of Belgium (Q31)
print(db.get_wikipedia_link("ja", "Q31"))

# Gel claims of Belgium (Q31)
print(db.get_claims("Q31"))

# Get all information of Belgium (Q31)
print(db.get_item("Q31"))

# Get redirect of Belgium (Q31)
redirects = db.get_redirect_of("Q31")
print(redirects)

# Get redirect of
print(db.get_redirect(redirects[0]))

# Get instance of Belgium (Q31)
instance_ofs = db.get_instance_of("Q31")
for i, wd_id in enumerate(instance_ofs):
    print(f"{i}: {wd_id} - {db.get_label(wd_id)}")

# Get subclass of Belgium (Q31)
print(db.get_subclass_of("Q31"))

# Get all types of Belgium (Q31)
types = db.get_all_types("Q31")
for i, wd_id in enumerate(types):
    print(f"{i}: {wd_id} - {db.get_label(wd_id)}")

# Print provenance list
def print_provenance_list(iter_obj):
    for i, provenance in enumerate(iter_obj):
        if i > 3:
            break
        subject = provenance["subject"]
        predicate = provenance["predicate"]
        value = provenance["value"]
        reference_node = provenance["reference"]
        print(
            f"{i+1}: <{subject}[{db.get_label(subject)}] - {predicate}[{db.get_label(predicate)}] - {value}>]]"
        )
        print(f"  Reference Node:")
        for ref_type, ref_objs in reference_node.items():
            for ref_prop, ref_v in ref_objs.items():
                print(f"    {ref_prop}[{db.get_label(ref_prop)}]: {ref_v}")
    print()


# Get provenance of Belgium (Q31)
print_provenance_list(db.iter_provenances("Q31"))
# Get provenance of Belgium (Q31), and Tokyo (Q1490)
print_provenance_list(db.iter_provenances(["Q31", "Q1490"]))
# Get provenance of all items
print_provenance_list(db.iter_provenances())

import time


def find_wikidata_items_haswbstatements(params, print_top=3):
    start = time.time()
    wd_ids = db.get_haswbstatements(params)
    end = time.time() - start
    print("Query:")
    for logic, prop, qid in params:
        if prop is None:
            prop_label = ""
        else:
            prop_label = f" - {prop}[{db.get_label(prop)}]"

        qid_label = db.get_label(qid)
        print(f"{logic}{prop_label}- {qid}[{qid_label}]")

    print(f"Answers: Found {len(wd_ids):,} items in {end:.5f}s")
    for i, wd_id in enumerate(wd_ids[:print_top]):
        print(f"{i+1}. {wd_id} - {db.get_label(wd_id)}")
    print(f"{4}. ...")
    print()


find_wikidata_items_haswbstatements(
    [
        # ??? - Graduate University for Advanced Studies
        [cf.ATTR_OPTS.AND, None, "Q2983844"]
    ]
)

find_wikidata_items_haswbstatements(
    [
        # instance of - human
        [cf.ATTR_OPTS.AND, "P31", "Q5"],
        # gender - male
        [cf.ATTR_OPTS.AND, "P21", "Q6581097"],
        # educated at - Todai
        [cf.ATTR_OPTS.AND, "P69", "Q7842"],
        # employer - Graduate University for Advanced Studies
        [cf.ATTR_OPTS.AND, "P108", "Q2983844"],
    ]
)

find_wikidata_items_haswbstatements(
    [
        # instance of - human
        [cf.ATTR_OPTS.AND, None, "Q5"],
        # gender - male
        [cf.ATTR_OPTS.AND, None, "Q6581097"],
        # educated at - Todai
        [cf.ATTR_OPTS.AND, None, "Q7842"],
        # employer - Graduate University for Advanced Studies
        [cf.ATTR_OPTS.AND, None, "Q2983844"],
    ]
)

find_wikidata_items_haswbstatements(
    [
        # ? - scholarly article
        [cf.ATTR_OPTS.AND, None, "Q13442814"],
        # ? - DNA
        [cf.ATTR_OPTS.OR, None, "Q7430"],
        # ? - X-ray diffraction
        [cf.ATTR_OPTS.OR, None, "Q12101244"],
        # ? - DNA
        [cf.ATTR_OPTS.OR, None, "Q911331"],
        # Francis Crick
        [cf.ATTR_OPTS.AND, None, "Q123280"],
        # ? - Nature
        [cf.ATTR_OPTS.AND, None, "Q180445"],
    ]
)

``` 

### 6. Todo
- Prepare downloadable indexed model link


### LICENSE
wikidb code is licensed under MIT License.

The LMDB lib is under OpenLDAP Public License (permissive software license)

Python binding: https://github.com/jnwatson/py-lmdb/blob/master/LICENSE

Original LMDB: https://github.com/LMDB/lmdb/blob/mdb.master/libraries/liblmdb/LICENSE
