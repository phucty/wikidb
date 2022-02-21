import os.path
import argparse
import requests
from tqdm.auto import tqdm
import config as cf
from core import io_worker as iw


def download_wikidata_dump(json_version=cf.DUMPS_WD_JSON, sql_version=cf.DUMPS_WD_SQL):
    urls = [
        f"https://dumps.wikimedia.org/wikidatawiki/{sql_version}/wikidatawiki-{sql_version}-page.sql.gz",
        f"https://dumps.wikimedia.org/wikidatawiki/{sql_version}/wikidatawiki-{sql_version}-redirect.sql.gz",
        f"https://dumps.wikimedia.org/wikidatawiki/entities/{json_version}/wikidata-{json_version}-all.json.bz2",
        # f"https://dumps.wikimedia.org/wikidatawiki/entities/{json_version}/wikidata-{json_version}-all.json.gz",
    ]

    for url in urls:
        dump_file = url.split("/")[-1]
        downloaded_file = f"{cf.DIR_DUMPS}/{dump_file}"

        if os.path.exists(downloaded_file):
            print(f"Downloaded: {downloaded_file}")
            continue
        iw.create_dir(downloaded_file)
        r = requests.get(url, stream=True)
        p_bar = tqdm(
            total=int(r.headers.get("content-length", 0)),
            unit="B",
            unit_scale=True,
            desc=dump_file,
        )
        with open(f"{cf.DIR_DUMPS}/{dump_file}", "wb") as f:
            for data in r.iter_content(10240):
                p_bar.update(len(data))
                f.write(data)
        p_bar.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--json_version",
        "-j",
        default=cf.DUMPS_WD_JSON,
        help="Wikidata Json dump version. Find at https://dumps.wikimedia.org/wikidatawiki/entities/. Default: 20220131",
    )
    parser.add_argument(
        "--sql_version",
        "-s",
        default=cf.DUMPS_WD_SQL,
        help="Wikidata SQL dump version. Find at https://dumps.wikimedia.org/wikidatawiki/. Default: 20220201",
    )
    args = parser.parse_args()
    download_wikidata_dump(**vars(args))

"""
wikidatawiki-20220201-page.sql.gz: 100%|████████████████████| 2.91G/2.91G [10:24<00:00, 4.66MB/s]
wikidatawiki-20220201-redirect.sql.gz: 100%|████████████████| 26.2M/26.2M [00:06<00:00, 4.31MB/s]
wikidata-20220131-all.json.bz2: 100%|█████████████████████| 72.4G/72.4G [3:58:37<00:00, 5.05MB/s]
"""
