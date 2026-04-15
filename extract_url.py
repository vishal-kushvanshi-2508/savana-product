


import json
# import requests
import re
from curl_cffi.requests import Session
from lxml import html

from store_data_database import *



head_url= "https://www.savana.com/category"

SESSION = Session(impersonate="chrome120")


API_URL = "https://api-shop-in.savana.com/n/api/buyer/guide/user/goods-flow/pageList"

session = Session(impersonate="chrome120")

headers = {
    "accept": "application/json, text/plain, */*",
    "content-type": "application/json;charset=UTF-8",
    "origin": "https://www.savana.com",
    "referer": "https://www.savana.com/",
    "user-agent": "Mozilla/5.0"
}


# HELPER: Extract JSON Array
def extract_array(text, key):
    start = text.find(f'"{key}"')
    start = text.find('[', start)

    bracket_count = 0
    array_start = None

    for i in range(start, len(text)):
        if text[i] == "[":
            if array_start is None:
                array_start = i
            bracket_count += 1
        elif text[i] == "]":
            bracket_count -= 1
            if bracket_count == 0:
                return text[array_start:i+1]

    return None

def process_goods(goods_list, category_dict):
    urls = []
    visited_ids = []

    for item in goods_list:
        goodsId = item.get("goodsId")
        goodsName = item.get("goodsName", "")

        if not goodsId:
            continue

        visited_ids.append(goodsId)

        image_colorId = item.get("imageList", [{}])[0].get("colorId")
        skcPGs = item.get("skcPGs")

        url = f"https://www.savana.com/details/{goodsName.lower().replace(' ', '-')}-{goodsId}?vid={image_colorId}&skcPGs={skcPGs}"


        # 🔥 FIX: create a copy
        new_dict = category_dict.copy()   # OR copy.deepcopy(category_dict)

        new_dict["product_url"] = url
        urls.append(new_dict)

        # print("2 now urls" , urls)

    # print("urls" , urls)
        # break

    return urls, visited_ids


def category_url_process(url, all_products, category_dict):
    url_id = url.rstrip("/").split("-")[-1]
    print("\n\nurl now  : ", url, "url_id : ", url_id)


    response = session.get(url)
    tree = html.fromstring(response.text)

    script = tree.xpath("//script[contains(text(),'window.$preData')]/text()")[0]

    goods_list_str = extract_array(script, "goodsList")
    goods_list = json.loads(goods_list_str)

    print(f"inner HTML Page -> {len(goods_list)} items")
    # print("category_dict : ", category_dict)

    # with open("final_urls4.json", "w", encoding="utf-8") as f:
    #     json.dump(goods_list, f, indent=4)

    # process first page
    urls, visited_ids = process_goods(goods_list, category_dict)
    all_products.extend(urls)


    # STEP 2: PAGINATION (API)
    page = 2   # start from next page

    while True:
        # print(f"\nFetching API Page: {page}")

        json_data = {
            "flowId": url_id,
            "flowType": "CATEGORY",
            "pageIndex": page,
            "pageSize": 20,
            "visitedGoodsIdList": visited_ids
        }

        response = session.post(API_URL, headers=headers, json=json_data)

        if response.status_code != 200:
            print("Request Failed ")
            break

        data = response.json()

        goods_list = data.get("data", {}).get("goodsList", [])

        #  STOP CONDITION
        if not goods_list:
            print("No more data ")
            break

        # print(f"API Page {page} -> {len(goods_list)} items")

        urls, new_ids = process_goods(goods_list, category_dict)

        all_products.extend(urls)
        visited_ids.extend(new_ids)
        with open("final_urls2.json", "a", encoding="utf-8") as f:
            json.dump(all_products, f, indent=4)

        if len(all_products) >= 500:
            merck_url_insert(list_data=all_products)
            all_products.clear()

        page += 1
    print("\nvisited_ids now :", len(visited_ids))



def fetch_urls():
    print("-----fetch_urls------")

    response = SESSION.get(
        head_url
    )

    with open("savana_response.html", "w", encoding='utf-8') as f:
        f.write(response.text)

    tree = html.fromstring(response.text)

    script = tree.xpath("//script[contains(text(),'window.$preData')]/text()")[0]

    categoryList_str = extract_array(script, "categoryList")
    categoryList = json.loads(categoryList_str)

    print(f"outer HTML Page -> {len(categoryList)} items")
    # print("done ")

    with open("category_url_data2.json", "w", encoding="utf-8") as f:
        json.dump(categoryList, f, indent=4)

    all_products = []
    for data in categoryList:
        id = data.get("id")
        title = data.get("title").lower()

        category_url = f"{head_url}/{title.lower()}-{id}"
        category_dict = {
            "category_id" : id,
            "category_name" : title,
            "category_url" : category_url,
            "product_url" : "",
            "status" : "pending"

        }

        ##  category_url_process
        category_url_process(category_url, all_products, category_dict)

        merck_url_insert(list_data=all_products)
        
        # break
