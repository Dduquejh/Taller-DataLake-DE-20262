
import requests
import yaml
from lxml import etree

with open("config/sources.yaml", encoding="utf-8") as f:
    sources = yaml.safe_load(f)["sources"]

for source in sources:
    try:
        response = requests.get(
            source["url"],
            timeout=20,
            headers={"User-Agent": "NoticiasDataLake/1.0"},
        )
        response.raise_for_status()

        root = etree.fromstring(response.content)
        items = root.xpath("//*[local-name()='item']")

        print(
            f'OK | {source["section"]} | '
            f'HTTP {response.status_code} | '
            f'{len(items)} noticias | '
            f'{len(response.content)} bytes'
        )
    except Exception as exc:
        print(f'ERROR | {source["section"]} | {exc}')

