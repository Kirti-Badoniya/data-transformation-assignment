#!/usr/bin/env python
# coding: utf-8

# In[5]:


import pandas as pd

# 1. Read the raw CSV into a DataFrame
def load_raw(path: str) -> pd.DataFrame:
    return pd.read_csv(path)

# 2a. Define a cleaning function for a DataFrame
def clean_sales(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Clean and convert types
    df['order_id'] = df['order_id'].astype(str).str.replace(r'\D', '', regex=True).fillna('0').astype(int)
    df['product_id'] = df['product_id'].astype(str).str.replace(r'\D', '', regex=True).fillna('0').astype(int)
    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').fillna(0).astype(int)
    df['price_per_unit'] = pd.to_numeric(df['price_per_unit'], errors='coerce').fillna(0.0).astype(float)

    # Parse dates with various formats
    df['order_date'] = pd.to_datetime(
        df['order_date'].astype(str),
        format=None, dayfirst=False, errors='coerce'
    ).fillna(pd.Timestamp('1970-01-01'))

    return df

# 2b. Define a UDF to compute total price
def add_total_price(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df['total_price'] = df['quantity'] * df['price_per_unit']
    return df

# 3. Pipeline runner
def process_sales(input_csv: str, output_csv: str) -> None:
    df = load_raw(input_csv)
    df = clean_sales(df)
    df = add_total_price(df)
    df.to_csv(output_csv, index=False)

if __name__ == '__main__':
    process_sales(r'C:\Users\Admin\Documents\Kirti\raw_sales.csv', r'C:\Users\Admin\Documents\Kirti\cleaned_sales.csv')


# In[ ]:




