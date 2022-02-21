# Import class
from core.db_wd import DBWikidata

# Wikidb
db = DBWikidata()


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
print_provenance_list(db.get_provenance_analysis("Q31"))
# Get provenance of Belgium (Q31), and Tokyo (Q1490)
print_provenance_list(db.get_provenance_analysis(["Q31", "Q1490"]))
# Get provenance of all items
print_provenance_list(db.get_provenance_analysis())

# import config as cf
# import time
#
#
# def find_wikidata_items_haswbstatements(params):
#     start = time.time()
#     wd_ids = db.get_haswbstatement(params)
#     end = time.time() - start
#     print(params)
#     print(f"Total: {len(wd_ids):,} in {end:.2f}s")
#     for i, wd_id in enumerate(wd_ids[:3]):
#         print(f"{i+1}. {wd_id} - {db.get_label(wd_id)}")
#     print(f"{4}. ...")
#     print()
#
#
# find_wikidata_items_haswbstatements([
#     # ??? - Graduate University for Advanced Studies
#     [cf.ATTR_OPTS.AND, None, "Q2983844"]
# ])
#
# find_wikidata_items_haswbstatements([
#         # instance of - human
#         [cf.ATTR_OPTS.AND, "P31", "Q5"],
#         # gender - male
#         [cf.ATTR_OPTS.AND, "P21", "Q6581097"],
#         # educated at - Todai
#         [cf.ATTR_OPTS.AND, "P69", "Q7842"],
#         # employer - Graduate University for Advanced Studies
#         [cf.ATTR_OPTS.AND, "P108", "Q2983844"],
#     ])
#
# find_wikidata_items_haswbstatements([
#         # instance of - human
#         [cf.ATTR_OPTS.AND, None, "Q5"],
#         # gender - male
#         [cf.ATTR_OPTS.AND, None, "Q6581097"],
#         # educated at - Todai
#         [cf.ATTR_OPTS.AND, None, "Q7842"],
#         # employer - Graduate University for Advanced Studies
#         [cf.ATTR_OPTS.AND, None, "Q2983844"],
#     ])
#
# find_wikidata_items_haswbstatements([
#         # ? - scholarly article
#         [cf.ATTR_OPTS.AND, None, "Q13442814"],
#         # ? - DNA
#         [cf.ATTR_OPTS.OR, None, "Q7430"],
#         # ? - X-ray diffraction
#         [cf.ATTR_OPTS.OR, None, "Q12101244"],
#         # ? - DNA
#         [cf.ATTR_OPTS.OR, None, "Q911331"],
#         # Francis Crick
#         [cf.ATTR_OPTS.AND, None, "Q123280"],
#         # ? - Nature
#         [cf.ATTR_OPTS.AND, None, "Q180445"],
#     ])
