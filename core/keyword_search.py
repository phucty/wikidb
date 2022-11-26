from time import time

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


class KeywordSearch:
    def __init__(self):
        self.URL = "https://mtab.app/api/v1/search"
        self.session = requests.Session()
        retries = Retry(total=5,
                        backoff_factor=1,
                        status_forcelist=[500, 502, 503, 504])
        self.session.mount('https://', HTTPAdapter(max_retries=retries))
        self.session.mount('http://', HTTPAdapter(max_retries=retries))

    def get(self, query_value, limit=20, mode="a", lang="en", expensive=0, info=0):
        query_args = {
            "q": query_value,
            "limit": limit,
            "m": mode,
            "lang": lang,
            "info": info,
            "expensive": expensive
        }
        start = time()
        responds = []
        if not query_value:
            return [], time() - start
        try:
            # tmp_responds = requests.get(self.URL, params=query_args)
            tmp_responds = self.session.get(self.URL, params=query_args)
            if tmp_responds.status_code == 200:
                tmp_responds = tmp_responds.json()
                if tmp_responds.get("hits"):
                    if info:
                        responds = [[r["id"], r["score"], r["label"], r["des"]] for r in tmp_responds["hits"]]
                    else:
                        responds = [[r["id"], r["score"]] for r in tmp_responds["hits"]]
        except Exception as message:
            print(f"\n{message}\n{str(query_args)}")
        run_time = time() - start
        return responds, run_time