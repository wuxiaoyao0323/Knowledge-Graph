import requests
from bs4 import BeautifulSoup
import time
from selenium import webdriver
import csv
from urllib.parse import urljoin
import re
import random
import pandas as pd
import json
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from bs4 import MarkupResemblesLocatorWarning
import warnings
warnings.filterwarnings("ignore", category=MarkupResemblesLocatorWarning)

# 配置信息
user_agents = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36 Edge/112.0.0.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36 Edge/16.16299',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36 Edge/96.0.1054.53',
    'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:64.0) Gecko/20100101 Firefox/64.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:54.0) Gecko/20100101 Firefox/54.0'
]

session = requests.Session()
base_url = 'https://art.nelson-atkins.org'
PROGRESS_FILE = "progress_nelson.json"
MAX_TOTAL = 2000

def get_random_user_agent():
    return random.choice(user_agents)


# 请求
def get_headers():
    return {
        "User-Agent": random.choice(user_agents),
        "Accept-Language": "en-US,en;q=0.9"
    }


# 断点续爬功能
def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"skip": 0, "count": 0}


def save_progress(skip, count):
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump({"skip": skip, "count": count}, f)

def download(args):
    url, oid = args
    if not url:
        return "", ""

    os.makedirs("images/nelson-atkins", exist_ok=True)
    path = f"images/nelson-atkins/{oid}.jpg"

    if os.path.exists(path):
        return path, url

    try:
        # 先试原图
        r = session.get(url, headers=get_headers(), timeout=15, stream=True)

        if r.status_code == 200:
            real_url = url
        else:
            if r.status_code != 200:
                return "", "" 

        with open(path, "wb") as f:
            for chunk in r.iter_content(1024):
                f.write(chunk)

        return path, real_url

    except:
        return "", ""

def parse_detail(session,url):
    # 通过字段标签提取详细信息 (基于 .detailField)
    def find_field(label):
        label_elem = soup.find("span", class_="detailFieldLabel", string=re.compile(label, re.I))
        if label_elem:
            val_elem = label_elem.find_next_sibling("span", class_="detailFieldValue")
            if val_elem:
                return val_elem.get_text(strip=True)
        return ""
    data = {
        'object_id': None,
        'title': None,
        'period': None,
        'type': None,
        'material': None,
        'description': "",
        'dimensions': "",
        'museum': None,
        'location': None,
        'detail_url': url,
        'image_url': "",
        "image_path": "",
        'credit_line': None,
        'accession_number': None,
        'crawl_date': datetime.now().strftime('%Y-%m-%d')
    }

    data['museum'] = "The Nelson-Atkins Museum of Art"
    data['location'] = "Kansas City, United States"

    r = session.get(url)
    if not r:
        return data

    soup = BeautifulSoup(r.text, "html.parser")

    # 获取描述
    container = soup.find("span", class_="textEntriesGallerylabel")
    
    if container:
        content_div = container.find("div", class_="detailField")
        if content_div:
            raw_text = content_div.get_text(" ", strip=True)
            raw_text = re.sub(r'^Gallery Label\s*', '', raw_text)
            data['description'] = re.sub(r'\s+', '', raw_text)
   


    script = soup.find("script", type="application/ld+json")
    json_data = {}
    if script:
        try:
            json_data = json.loads(script.string)
            # 优先填充 ID 和名称
            data['object_id'] = json_data.get('identifier') # 如果有 ID
            data['title'] = json_data.get('name', '')
            if not data['description'] and 'description' in json_data:
                data['description'] = json_data['description']
        except:
            pass

  
    if not data['title']:
        title_tag = soup.find("h1", itemprop="name")
        if title_tag:
            data['title'] = title_tag.get_text(strip=True)

    
    
    


    # 映射字段
    data['accession_number'] = find_field("Object number") or find_field("Object Number")
    if data['accession_number']:
        data['object_id'] = data['accession_number']
    data['credit_line'] = find_field("Credit Line")
    data['period'] = find_field("Date") or find_field("Date Created")
    data['material'] = find_field("Medium") 
    data['dimensions'] = find_field("Dimensions")
    

    # 获取 Type
    terms_container = soup.find("div", class_="thesconceptsField")
    type_list = []
    if terms_container:
        # 找到所有分类链接文本
        term_spans = terms_container.find_all("span")
        for span in term_spans:
            text = span.get_text(strip=True)
            if text.lower() not in ['chinese', 'terms','japanese', 'korean']:
                type_list.append(text)
    # 将提取到的类型用逗号连接，或者取前两个主要类型
    if type_list:
        main_types=type_list[:2]
        data['type'] = ", ".join(main_types)
    
    

    #  图片提取
    # 策略1: 尝试提取高清原图 (查看 "Download" 链接)
    download_link = soup.find("a", href=re.compile("/internal/media/dispatcher/\d+/full"))
    if download_link:
        full_url_path = download_link['href']
        # 拼接完整 URL
        data['image_url'] = "https://art.nelson-atkins.org" + full_url_path
    else:
        # 策略2: 使用 Open Graph 图片 (通常是预览大图)
        og_img = soup.find("meta", property="og:image")
        if og_img:
            data['image_url'] = og_img['content']

    
    
    # 8. object_id 备用方案
    # 如果 JSON 中没有 ID，使用 URL 中的对象 ID 或 Accession Number
    if not data['object_id']:
        # 从 URL提取: /objects/3469/... -> 3469
        obj_id_match = re.search(r'/objects/(\d+)', url)
        if obj_id_match:
            data['object_id'] = obj_id_match.group(1)

    return data


# 主爬取函数
def crawl_penn():
    data = []
    seen = set()
    progress = load_progress()
    skip = progress["skip"]
    count = progress["count"]

    driver = webdriver.Chrome()
    driver.get("https://art.nelson-atkins.org")

    # 2. 等待几秒让 Cloudflare 验证通过（如果有的话）
    time.sleep(10) 

    # 3. 获取通过验证后的 Cookie
    cookies = driver.get_cookies()
    driver.quit()

    # 4. 将 Cookie 转换为 requests 可用的格式
    session = requests.Session()
    for cookie in cookies:
        session.cookies.set(cookie['name'], cookie['value'])

    museum_name = "The Nelson-Atkins Museum of Art"
    location = "4525 Oak St. Kansas City, MO 64111"

    print("\n开始爬取中国文物...\n")

    page = skip+1
    

    while True:
        if count >= MAX_TOTAL:
            print(f"\n已达到最大爬取数量 {MAX_TOTAL}，爬取结束。")
            break

        print(f"\n正在爬取第 {page} 页...")

        try:
            headers = {
                'User-Agent': get_random_user_agent(),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
            }
            url=f"{base_url}/advancedsearch/Objects/thesadvsearchCulturenationality%3Ahttp%25255C%3A%252F%252Fnodes.emuseum.com%252FCNJ74X1F%252Fapis%252Femuseum%252Fnetwork%252Fv1%252Fvocabularies%252FtermMaster1545150/images?page={page}"
            response = session.get(url, headers=headers)
            if not response:
                print("页面请求失败，可能被屏蔽，等待后重试...")
                time.sleep(random.uniform(5, 10))
                continue
            
            soup = BeautifulSoup(response.text, 'html.parser')

            # 1.找到所有的文物卡片
            items = soup.select('div.result.item')
            print(f"当前页共找到 {len(items)} 个文物。")

            batch_records = []
            download_tasks = []
            # 2. 遍历每个文物卡片，提取详情页链接
            for i, item in enumerate(items):
                if count >= MAX_TOTAL:
                        break
                
                link_tag = item.select_one('div.title.text-wrap a')
                #去重
                if  link_tag in seen:
                    continue
                
                if link_tag and link_tag.get('href'):
                    detail_url = link_tag['href']
                    full_url = base_url + detail_url
                else:
                    print(f"文物 {i+1}: 未找到链接")
                    continue
                
                record = parse_detail(session,full_url)
                batch_records.append(record)
                download_tasks.append((record['image_url'], record['object_id']))

                seen.add(record['object_id'])
                seen.add(link_tag)
                print(f"[{count}] {record['title']}")
                
                count=count+1
            # 多线程下载图片
            if download_tasks:
                print(f"\n开始下载本批次 {len(download_tasks)} 张图片...")
                with ThreadPoolExecutor(max_workers=4) as executor:
                    results = list(executor.map(download, download_tasks))

                success_count = 0

                for i in range(len(batch_records)):
                    path, real_url = results[i]
                    batch_records[i]["image_path"] = path

                    if path:
                        success_count += 1
                print(f"本批次图片下载完成，成功 {success_count} 张\n")
            
            data.extend(batch_records)
            skip = page
            save_progress(skip, count)

            page=page+1
            time.sleep(random.uniform(1, 2))
                

        except requests.exceptions.RequestException as e:
            print(f"请求出错: {e}")
            time.sleep(random.uniform(3, 5))
            continue

    df = pd.DataFrame(data).drop_duplicates(subset=["object_id"], keep="first")
    return df


if __name__ == "__main__":
    df = crawl_penn()
    output_file = "nelson-atkins_museum.csv"
    df.to_csv(output_file, index=False, encoding="utf-8-sig")
    print(f"\n所有数据已保存到 {output_file}")
