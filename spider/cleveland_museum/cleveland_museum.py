import requests
import pandas as pd
import time
import random
import json
import os
import re
from datetime import datetime
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor



# UA池
user_agents = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/135.0.0.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/112.0.0.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Chrome/96.0.4664.110',
    'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:64.0) Firefox/64.0'
]

session = requests.Session()



# 断点文件
PROGRESS_FILE = "progress.json"


def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"skip": 0, "count": 0}


def save_progress(skip, count):
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump({"skip": skip, "count": count}, f)



# HTML清洗
def clean_html(text):
    if not text:
        return ""
    text = str(text)
    text = BeautifulSoup(text, "html.parser").get_text()
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def clean(value):
    if value is None:
        return ""
    if isinstance(value, list):
        return " | ".join([clean(i) for i in value])
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False)
    return clean_html(value)



# material 提取
def extract_material(item):
    return clean(
        item.get("material")
        or item.get("materials")
        or item.get("technique")
        or item.get("medium")
        or ""
    )



# type 提取
def extract_type(item):
    return clean(
        item.get("classification")
        or item.get("type")
        or item.get("object_type")
        or item.get("technique")
        or ""
    )



# 请求JSON
def request_json(url, retries=3):
    for _ in range(retries):
        try:
            r = session.get(
                url,
                headers={"User-Agent": random.choice(user_agents)},
                timeout=15
            )
            r.raise_for_status()
            return r.json()
        except:
            time.sleep(random.uniform(1, 2))
    return None



# 提取 credit_line
def extract_credit_line(item):
    text = (
        item.get("creditline")
        or item.get("credit_line")
        or item.get("tombstone")
        or ""
    )
    return clean(text)

# 安全提取图片URL
def extract_image_url(item):
    try:
        return (
            item.get("images", {})
            .get("web", {})
            .get("url", "")
        ) or ""
    except:
        return ""


# 图片下载
def download_image(args):
    url, img_id, museum = args

    if not url:
        return ""

    folder = f"images/{museum}"
    os.makedirs(folder, exist_ok=True)

    path = os.path.join(folder, f"{img_id}.jpg")

    if os.path.exists(path):
        return path

    try:
        r = session.get(url, stream=True, timeout=(2, 4))
        if r.status_code != 200:
            return ""

        with open(path, "wb") as f:
            for chunk in r.iter_content(1024):
                f.write(chunk)

        return path

    except:
        return ""



# 爬虫
def crawl_cleveland():
    data = []

    progress = load_progress()
    skip = progress["skip"]
    count = progress["count"]

    limit = 100
    max_total = 2500

    museum_name = "The Cleveland Museum of Art"
    location = "Cleveland, Ohio, United States"

    print("\n[Cleveland克利夫兰] 开始爬取...\n")

    while count < max_total:

        url = f"https://openaccess-api.clevelandart.org/api/artworks/?limit={limit}&skip={skip}"
        res = request_json(url)

        if not res:
            break

        items = res.get("data", [])
        if not items:
            break

        batch_records = []
        download_tasks = []

        for item in items:

            if count >= max_total:
                break

            culture = str(item.get("culture", ""))
            if "China" not in culture and "Chinese" not in culture:
                continue

            img_url = extract_image_url(item)
            object_id = item.get("id")

            record = {
                "object_id": clean(object_id),
                "title": clean(item.get("title")),
                "period": clean(item.get("creation_date")),
                "type": extract_type(item),
                "material": extract_material(item),
                "description": clean(item.get("description")),
                "dimensions": clean(item.get("measurements")),
                "museum": museum_name,
                "location": location,
                "detail_url": clean(item.get("url")),
                "image_url": img_url,
                "image_path": "",

                "credit_line": extract_credit_line(item),

                "accession_number": clean(item.get("accession_number")),
                "crawl_date": datetime.now().strftime("%Y-%m-%d")
            }

            batch_records.append(record)
            download_tasks.append((img_url, object_id, "cleveland"))

            count += 1
            print(f"[{count}] {record['title']}")

        with ThreadPoolExecutor(max_workers=10) as executor:
            results = list(executor.map(download_image, download_tasks))

        for i in range(len(batch_records)):
            batch_records[i]["image_path"] = results[i]

        data.extend(batch_records)

        skip += limit
        save_progress(skip, count)

        time.sleep(random.uniform(0.5, 1.2))

    return pd.DataFrame(data).drop_duplicates()



# 统计函数
def generate_stats(df):
    total = len(df)

    stats = {
        "museum": "The Cleveland Museum of Art",
        "total": total,
        "image_success_rate": round((df["image_url"] != "").sum() / total * 100, 2) if total else 0,
        "field_completeness": {
            "object_id": f"{round((df['object_id'] != '').sum() / total * 100, 2)}%",
            "title": f"{round((df['title'] != '').sum() / total * 100, 2)}%",
            "period": f"{round((df['period'] != '').sum() / total * 100, 2)}%",
            "type": f"{round((df['type'] != '').sum() / total * 100, 2)}%",
            "material": f"{round((df['material'] != '').sum() / total * 100, 2)}%",
            "description": f"{round((df['description'] != '').sum() / total * 100, 2)}%",
            "detail_url": f"{round((df['detail_url'] != '').sum() / total * 100, 2)}%",
            "image_url": f"{round((df['image_url'] != '').sum() / total * 100, 2)}%",
            "image_path": f"{round((df['image_path'] != '').sum() / total * 100, 2)}%",
            "crawl_date": f"{round((df['crawl_date'] != '').sum() / total * 100, 2)}%"
        }
    }

    return stats



# 主函数
if __name__ == "__main__":

    df = crawl_cleveland()

    df.to_csv(
        "Cleveland_museum.csv",
        index=False,
        encoding="utf-8-sig"
    )

    stats = generate_stats(df)

    print("数据统计结果")
  
    print(f"博物馆: {stats['museum']}")
    print(f"总数量: {stats['total']}")
    print(f"图片成功率: {stats['image_success_rate']}%")

    print("\n字段完整率:")
    print(json.dumps(stats["field_completeness"], indent=2, ensure_ascii=False))
