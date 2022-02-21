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
