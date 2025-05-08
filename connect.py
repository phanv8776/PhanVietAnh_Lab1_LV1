import requests
import psycopg2
from config import load_config
import json
import os

folder_path = r"C:\Users\Admin\tiki_env\output"

def getProduct(product_id):
    file_name = f"products_{product_id}.json"
    file_path = os.path.join(folder_path, file_name)

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Không tìm thấy file: {file_path}")

    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)

def insert_product(product,cur):
    global conn
    sql = """
    INSERT INTO tiki_products (
        id, name, short_description, price, original_price,
        rating_average, review_count, quantity_sold_text,
        brand_name, categories_name_path
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (id) DO NOTHING;
    """

    data = (
        product['id'],
        product['name'],
        product.get('short_description', ''),
        product['price'],
        product.get('original_price', 0),
        product.get('rating_average', 0.0),
        product.get('review_count', 0),
        product.get('quantity_sold', {}).get('text', ''),
        product.get('brand', {}).get('name', ''),
        " > ".join([c['name'] for c in product.get('categories', [])])
    )

    cur.execute(sql, data)


def process_all_files():
    params = load_config()
    conn = psycopg2.connect(**params)
    cur = conn.cursor()

    file_count = 0
    product_count = 0

    for filename in os.listdir(folder_path):
        if filename.endswith(".json"):
            file_path = os.path.join(folder_path, filename)
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    products = json.load(f)
                    for product in products:
                        insert_product(product, cur)
                        product_count += 1
                    file_count += 1
            except Exception as e:
                print(f"Lỗi đọc file {filename}: {e}")

    conn.commit()
    cur.close()
    conn.close()
    print(f"Đã xử lý {file_count} file và ghi {product_count} sản phẩm vào DB.")

if __name__ == "__main__":
    process_all_files()
