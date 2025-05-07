import requests
import psycopg2
from config import load_config

def getProduct(product_id):
    url = f"https://api.tiki.vn/product-detail/api/v1/products/{product_id}"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json"
    }
    response = requests.get(url,headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Lỗi API:{response.status_code}- {response.text}")

def insert_product(product):
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

    try:
        params = load_config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(sql, data)
        conn.commit()
        cur.close()
        print("Đã ghi vào DB.")
    except Exception as e:
        print(f"Lỗi DB: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    products = getProduct(138083218)
    print(products)
    insert_product(products)
    print(products)
