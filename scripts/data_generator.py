import os
import random
from datetime import datetime, timedelta
import pandas as pd
from faker import Faker

fake = Faker("id_ID")

PRODUCT_COUNT = random.randint(50, 75)
CUSTOMERS_PER_DAY = random.randint(25, 80)
ORDERS_PER_DAY = random.randint(100, 200)
DAYS = 7

OUTPUT_DIR = "/opt/airflow/data/raw"  
os.makedirs(OUTPUT_DIR, exist_ok=True)


def ensure_dir(path: str):
    try:
        if not os.path.exists(path):
            os.makedirs(path)
    except Exception as e:
        raise RuntimeError(f"Failed to create directory {path}: {e}")


def safe_to_csv(df: pd.DataFrame, path: str):
    try:
        df.to_csv(path, index=False)
    except Exception as e:
        raise RuntimeError(f"Failed to write CSV {path}: {e}")

# GENERATORS

def gen_products(n=PRODUCT_COUNT):
    try:
        rows = []
        categories = ["electronics", "fashion", "groceries"]
        product_names = {
            "electronics": ["Laptop", "Smartphone", "Tablet", "Computer", "Camera"],
            "fashion": ["T-Shirt", "Jeans", "Sneakers", "Jacket", "Dress"],
            "groceries": ["Coffee", "Vegetables", "Fruit", "Rice", "Meat"]
        }
        price_ranges = {
            "electronics": (1500000, 30000000),  
            "fashion": (100000, 3000000),
            "groceries": (5000, 200000)
        }
        suffixes = ["Original", "Special", "Limited", "Edition", "Discount"]

        for pid in range(1, n + 1):
            cat = random.choice(categories)
            pname = random.choice(product_names[cat])
            model_suffix = random.choice(suffixes)
            price = round(random.uniform(*price_ranges[cat]))
            cost = round(price * random.uniform(0.5, 0.9))

            rows.append({
                "product_id": pid,
                "sku": f"SKU-{pid:04d}",
                "name": f"{pname} {model_suffix}",
                "category": cat,
                "price": price,
                "current_cost": cost
            })
        return pd.DataFrame(rows)
    except Exception as e:
        raise RuntimeError(f"Error generating products: {e}")


def gen_customers(n, start_id):
    try:
        rows = []
        for i in range(n):
            cid = start_id + i
            dob = fake.date_of_birth(minimum_age=18, maximum_age=70)
            age = (datetime.today().date() - dob).days // 365
            rows.append({
                "customer_id": cid,
                "email": fake.email(),
                "full_name": fake.name(),
                "signup_date": fake.date_between(start_date='-1y', end_date='today'),
                "phone": fake.phone_number(),
                "status": random.choice(["active", "inactive"]),
                "dob": dob,
                "age": age
            })
        return pd.DataFrame(rows)
    except Exception as e:
        raise RuntimeError(f"Error generating customers: {e}")


def gen_addresses(customers):
    try:
        rows = []
        aid = 1
        for _, cust in customers.iterrows():
            rows.append({
                "address_id": aid,
                "customer_id": cust.customer_id,
                "address_line1": fake.street_address(),
                "city": fake.city(),
                "state": fake.state(),
                "postal_code": fake.postcode(),
                "country": "Indonesia",
            })
            aid += 1
        return pd.DataFrame(rows)
    except Exception as e:
        raise RuntimeError(f"Error generating addresses: {e}")


def gen_orders(customers, day, start_id, addresses):
    try:
        rows = []
        for oid in range(start_id, start_id + ORDERS_PER_DAY):
            cust = customers.sample(1).iloc[0]
            addr = addresses[addresses.customer_id == cust.customer_id].sample(1).iloc[0]
            rows.append({
                "order_id": oid,
                "customer_id": cust.customer_id,
                "order_date": day,
                "order_status": random.choice(["pending", "shipped", "completed", "cancelled"]),
                "shipping_address_id": addr.address_id,
                "billing_address_id": addr.address_id,
                "total_amount": 0,  
            })
        return pd.DataFrame(rows)
    except Exception as e:
        raise RuntimeError(f"Error generating orders: {e}")


def gen_order_items(orders, products, start_id):
    try:
        rows = []
        item_id = start_id
        for _, order in orders.iterrows():
            num_items = random.randint(1, 3)
            for _ in range(num_items):
                prod = products.sample(1).iloc[0]
                qty = random.randint(1, 3)
                total = qty * prod.price
                rows.append({
                    "order_item_id": item_id,
                    "order_id": order.order_id,
                    "product_id": prod.product_id,
                    "quantity": qty,
                    "unit_price": prod.price,
                    "item_total": total,
                })
                item_id += 1
        return pd.DataFrame(rows)
    except Exception as e:
        raise RuntimeError(f"Error generating order items: {e}")


def gen_payments(orders, start_id):
    try:
        rows = []
        pid = start_id
        for _, order in orders.iterrows():
            status = random.choices(["success", "failed"], weights=[0.9, 0.1])[0]
            amount = order.total_amount if status == "success" else 0.0
            rows.append({
                "payment_id": pid,
                "order_id": order.order_id,
                "payment_date": order.order_date,
                "payment_method": random.choice(["CreditCard", "QRIS", "DebitCard", "GoPay", "SPay"]),
                "amount": amount,
                "status": status,
            })
            pid += 1
        return pd.DataFrame(rows)
    except Exception as e:
        raise RuntimeError(f"Error generating payments: {e}")


if __name__ == "__main__":
    try:
        ensure_dir(OUTPUT_DIR)

        # generate static products
        products = gen_products()
        safe_to_csv(products, f"{OUTPUT_DIR}/products.csv")

        # mulai ID dari angka random
        customer_start_id = random.randint(1000, 5000)
        order_start_id = random.randint(10000, 50000)
        order_item_start_id = random.randint(50000, 100000)
        payment_start_id = random.randint(10000, 70000)

        today = datetime.today().date()
        for d in range(DAYS):
            current_date = today - timedelta(days=d)
            folder = f"{OUTPUT_DIR}/{current_date}"
            ensure_dir(folder)

            # generate customers & addresses
            customers = gen_customers(random.randint(25, 50), customer_start_id)
            addresses = gen_addresses(customers)
            customer_start_id += len(customers)

            # generate orders
            orders = gen_orders(customers, current_date, order_start_id, addresses)
            order_start_id += len(orders)

            # generate order items
            order_items = gen_order_items(orders, products, order_item_start_id)
            order_item_start_id += len(order_items)

            # update total_amount
            totals = order_items.groupby("order_id")["item_total"].sum().reset_index()
            orders = orders.merge(totals, on="order_id", how="left")
            orders["total_amount"] = orders["item_total"].fillna(0)
            orders.drop(columns=["item_total"], inplace=True)

            # generate payments
            payments = gen_payments(orders, payment_start_id)
            payment_start_id += len(payments)

            # save CSV
            safe_to_csv(customers, f"{folder}/customers.csv")
            safe_to_csv(addresses, f"{folder}/addresses.csv")
            safe_to_csv(orders, f"{folder}/orders.csv")
            safe_to_csv(order_items, f"{folder}/order_items.csv")
            safe_to_csv(payments, f"{folder}/payments.csv")

            print(f"✅ Generated data for {current_date} -> {folder}")

    except Exception as e:
        print(f"❌ Pipeline failed: {e}")

