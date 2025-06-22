#!/usr/bin/env python
# coding: utf-8

# In[8]:


import pandas as pd
import sqlite3

# Load CSVs
df1 = pd.read_csv(r'C:\Users\Admin\Documents\Kirti\products_day1.csv')
df2 = pd.read_csv(r'C:\Users\Admin\Documents\Kirti\products_day2.csv')

#  2. Create in-memory SQLite DB
conn = sqlite3.connect(':memory:')
df1.to_sql('day1', conn, if_exists='replace', index=False)
df2.to_sql('day2', conn, if_exists='replace', index=False)

#  3. Full-row changes (Added or Removed, one row per product_id)
full_sql = """
WITH ids1 AS (SELECT product_id FROM day1),
     ids2 AS (SELECT product_id FROM day2)

SELECT d1.product_id, d1.name, d1.category, d1.price, d1.stock, 'REMOVED' AS change_type
  FROM day1 AS d1
  LEFT JOIN ids2 USING(product_id)
 WHERE ids2.product_id IS NULL

UNION ALL

SELECT d2.product_id, d2.name, d2.category, d2.price, d2.stock, 'ADDED' AS change_type
  FROM day2 AS d2
  LEFT JOIN ids1 USING(product_id)
 WHERE ids1.product_id IS NULL;
"""

full_changes = pd.read_sql_query(full_sql, conn)

#  4. Column-level differences for common product_ids
cols = [c for c in df1.columns if c != 'product_id']
col_sql = " UNION ALL ".join(f"""
SELECT
  d1.product_id,
  '{c}' AS column_name,
  CAST(d1.{c} AS TEXT) AS old_value,
  CAST(d2.{c} AS TEXT) AS new_value
FROM day1 d1
JOIN day2 d2 USING(product_id)
WHERE d1.{c} != d2.{c}
""" for c in cols)

col_changes = pd.read_sql_query(col_sql, conn)

#  5. Results
print(" Full-Row Changes:")
print(full_changes.to_string(index=False))

print("\n Column-Level Changes:")
print(col_changes.to_string(index=False))

#  6. Clean up
conn.close()



# In[ ]:




