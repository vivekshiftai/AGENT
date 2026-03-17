"""Debug: check why ProcessOrderBuildNode produces 0 process orders."""
from clickhouse_connect import get_client

c = get_client(
    host='135.237.184.199', port=8123, database='default',
    username='default', password='123', interface='http'
)

# Get work order product IDs
r = c.query("""
SELECT DISTINCT product_id, product_name
FROM chg_work_orders
WHERE status IN ('PLANNED','IN_PROGRESS')
ORDER BY product_id
""")
wo_products = {row[0] for row in r.result_rows}
print(f"Work order products ({len(wo_products)}):")
for row in r.result_rows:
    print(f"  {row[0]}: {row[1]}")
print()

# Get recipe product IDs
r2 = c.query("""
SELECT DISTINCT product_id
FROM chg_recipe_header
WHERE status = 'ACTIVE'
ORDER BY product_id
""")
recipe_products = {row[0] for row in r2.result_rows}
print(f"Recipe products ({len(recipe_products)}):")
for row in r2.result_rows:
    print(f"  {row[0]}")
print()

# Find matches
matches = wo_products & recipe_products
print(f"Matching products ({len(matches)}):")
for p in sorted(matches):
    print(f"  {p}")

missing = wo_products - recipe_products
print(f"\nWork orders WITHOUT recipe ({len(missing)}):")
for p in sorted(missing):
    print(f"  {p}")

c.close()
