# Import class
from core.db_wd import DBWikidata

# Wikidb
db = DBWikidata()

### 1. Get Entity Information

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

# Get properties between two Wikidata items
properties = db.get_properties_from_head_qid_tail_qid("Q1490", "Q17")
for i, wd_id in enumerate(properties):
    print(f"{i+1}: {wd_id} - {db.get_label(wd_id)}")

### 2. Get Provenance nodes

# Print provenance list
def print_provenance_list(iter_obj, top=3):
    for i, provenance in enumerate(iter_obj):
        if i > top:
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

# Wikidata provenances stats

from collections import Counter
from tqdm.notebook import tqdm

c_entities = 0
c_facts = 0
c_refs = 0
ref_types = Counter()
ref_props = Counter()
ref_props_c = 0
ref_types_c = 0


def update_desc():
    return f"Facts:{c_facts:,}|Refs:{c_refs:,}"


step = 10000
for wd_id, claims in tqdm(db.iter_item_provenances(), total=db.size()):
    c_entities += 1
    for claim_type, claim_objs in claims.items():
        for claim_prop, claim_values in claim_objs.items():
            for claim_value in claim_values:
                c_facts += 1
                refs = claim_value.get("references")
                if not refs:
                    continue
                for reference_node in refs:
                    c_refs += 1
                    for ref_type, ref_objs in reference_node.items():
                        ref_types_c += 1
                        ref_types[ref_type] += 1
                        for ref_prop in ref_objs.keys():
                            ref_props_c += 1
                            ref_props[ref_prop] += 1

print("Reference node stats")
print(f"Items: {c_entities:,} entities")
print(f"Facts: {c_facts:,} facts, {c_facts/c_entities:.2f} facts/entity")
print(f"References: {c_refs:,} references, {c_refs/c_facts:.2f} references/fact")

print("\nReference stats:")
print(f"Types/reference: {ref_props_c / c_refs:.2f}")
print(f"Properties/reference: {ref_props_c / c_refs:.2f}")


def print_top(counter_obj, total, top=100, message="", get_label=False):
    print(f"Top {top} {message}: ")
    top_k = sorted(counter_obj.items(), key=lambda x: x[1], reverse=True)[:top]
    for i, (obj, obj_c) in enumerate(top_k):
        if get_label:
            obj = f"{obj}\t{db.get_label(obj)}"
        print(f"{i+1}\t{obj_c:,}\t{obj_c/total*100:.2f}%\t{obj}")


print_top(ref_types, total=c_refs, message="types")
print_top(ref_props, total=c_refs, message="properties", get_label=True)

### 3. Entities boolean search
# Find subset of entities (head entities) with information about tail entities and properties (triples: <head entities, property, tail entities>)

import time
import config as cf


def find_wikidata_items_haswbstatements(params, print_top=3, get_qid=True):
    start = time.time()
    wd_ids = db.get_haswbstatements(params, get_qid=get_qid)
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


print("1.1. Get all female (Q6581072)")
find_wikidata_items_haswbstatements([[cf.ATTR_OPTS.AND, None, "Q6581072"]])

print("1.1. Get all female (Q6581072)")
find_wikidata_items_haswbstatements(
    [[cf.ATTR_OPTS.AND, None, "Q6581072"]], get_qid=False
)

print("1.2. Get all male (Q6581072)")
find_wikidata_items_haswbstatements([[cf.ATTR_OPTS.AND, None, "Q6581097"]])

print("1.2. Get all male (Q6581072)")
find_wikidata_items_haswbstatements(
    [[cf.ATTR_OPTS.AND, None, "Q6581097"]], get_qid=False
)

print(
    "2. Get all entities has relation with Graduate University for Advanced Studies (Q2983844)"
)
find_wikidata_items_haswbstatements(
    [
        # ??? - Graduate University for Advanced Studies
        [cf.ATTR_OPTS.AND, None, "Q2983844"]
    ]
)

print(
    "3. Get all entities who are human, male, educated at Todai, and work at SOKENDAI"
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

print("4. Get all entities that have relation with human, male, Todai, and SOKENDAI")
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

print(
    "5. Get all entities that have relation with scholarly article or DNA, X-ray diffraction, and Francis Crick and Nature"
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
