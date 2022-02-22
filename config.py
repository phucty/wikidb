from datetime import datetime
import psutil

# Project directory
DIR_ROOT = "/Users/phucnguyen/git/wikidb"

DUMPS_WD_JSON = "20220131"
DUMPS_WD_SQL = "20220201"

DIR_DUMPS = f"{DIR_ROOT}/data/dump"

DIR_DUMP_WD = f"{DIR_DUMPS}/wikidata-{DUMPS_WD_JSON}-all.json.gz"
DIR_DUMP_WIKIDATA_PAGE = f"{DIR_DUMPS}/wikidatawiki-{DUMPS_WD_SQL}-page.sql.gz"
DIR_DUMP_WIKIDATA_REDIRECT = f"{DIR_DUMPS}/wikidatawiki-{DUMPS_WD_SQL}-redirect.sql.gz"

DIR_MODELS = f"{DIR_ROOT}/data/models"
DIR_WIKIDATA_ITEMS_JSON = f"{DIR_MODELS}/wikidb.lmdb"
DIR_WIKIDATA_ITEMS_TRIE = f"{DIR_MODELS}/wikidb.trie"
DIR_WIKIDATA_ITEMS_PAGE = f"{DIR_MODELS}/wikidb.page"

# Log
FORMAT_DATE = "%Y_%m_%d_%H_%M"
DIR_LOG = f"{DIR_ROOT}/log/{datetime.now().strftime(FORMAT_DATE)}.txt"

# Configuration
ENCODING = "utf-8"

SIZE_1MB = 1_048_576
SIZE_512MB = 536_870_912
SIZE_1GB = 1_073_741_824

LMDB_MAX_KEY = 511
LMDB_MAP_SIZE = 10_737_418_240  # 10GB
# Using Ram as buffer
LMDB_BUFF_BYTES_SIZE = psutil.virtual_memory().total // 10
if LMDB_BUFF_BYTES_SIZE > SIZE_1GB:
    LMDB_BUFF_BYTES_SIZE = SIZE_1GB
# LMDB_BUFF_BYTES_SIZE = SIZE_1MB * 10


# Enum
class ToBytesType:
    OBJ = 0
    INT_NUMPY = 1
    INT_BITMAP = 2


class DBUpdateType:
    SET = 0
    COUNTER = 1


class ATTR_OPTS:
    AND = "AND"
    OR = "OR"
    NOT = "NOT"


WD = "http://www.wikidata.org/entity/"
WDT = "http://www.wikidata.org/prop/direct/"


WIKIDATA_IDENTIFIERS = {
    "Q4167410",  # Disambiguate page
    "Q4167836",  # category
    "Q24046192",  # category stub
    "Q20010800",  # user category
    "Q11266439",  # template
    "Q11753321",  # navigational template
    "Q19842659",  # user template
    "Q21528878",  # redirect page
    "Q17362920",  # duplicated page
    "Q14204246",  # project page
    "Q21025364",  # project page
    "Q17442446",  # internal item
    "Q26267864",  # KML file
    "Q4663903",  # portal
    "Q15184295",  # module
}
