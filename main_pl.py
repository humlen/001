# Fetch Libraries
import datetime
import glob
import time

import matplotlib.pyplot as plt
import polars as pl
import yfinance as yf
from statsmodels import api as sm
from tqdm import tqdm

# Keep for debugging
#with pl.Config(tbl_cols=df.width):
#    print(df)

# Timer Function
def timer(func):
    def wrapper():
        t1 = time.time()
        func()
        t2 = time.time()-t1
        print(f"{func.__name__} ran in {t2} seconds")
    return wrapper

# Local libs and structs
FOLDER_PATH = (
    r"C:\Users\eirik\Codebase\001 Compounding Linear Relationships\data\sample data"
)
ALL_FILES = glob.glob(FOLDER_PATH + "/*.csv")
Ratios = []
Multiples = []
Stocks = []
Prices = []

# Populate DFs

for filename in tqdm(ALL_FILES):
    # Determine the filetype
    name = filename.split("sample data\\")[1].split(".csv")[0]
    if "multiples" in name:
        df = pl.read_csv(filename)
        ticker = name.split("multiples_")[1]
        df = df.with_columns(pl.lit(ticker).alias("Ticker"))
        df = df.rename(
            {
                "NTM Revenues\xa0": "NTM Revenues",
                "NTM Normalized Earnings Per Share\xa0": "NTM EPS",
                "LTM Diluted EPS Before Extra\xa0": "LTM EPS",
                "NTM Price / Sales (P/S)": "NTM P/S",
                "NTM Price / Normalized Earnings (P/E)": "NTM P/E",
                "LTM Price / Sales (P/S)": "LTM P/S", "LTM Price / Diluted EPS (P/E)\xa0": "LTM P/E",
            }
        )
        df = df.select(
            pl.col("Date").str.to_date("%d/%m/%y"),
            "Ticker",
            pl.col("NTM P/S").str.replace("x","").str.replace("\)","").str.replace("\(","-").cast(pl.Float32),
            pl.col("NTM P/E").str.replace("x","").str.replace("\)","").str.replace("\(","-").cast(pl.Float32),
            pl.col("LTM P/S").str.replace("x","").str.replace("\)","").str.replace("\(","-").cast(pl.Float32),
            pl.col("LTM P/E").str.replace("x","").str.replace("\)","").str.replace("\(","-").cast(pl.Float32),
            pl.col("NTM Revenues").cast(pl.Float32),
            pl.col("LTM Revenues").cast(pl.Float32),
            "NTM EPS",
            "LTM EPS",
        )
        Multiples.append(df)
    if "ratios" in name:
        df = pl.read_csv(filename)
        ticker = name.split("ratios_")[1]
        df = df.with_columns(pl.lit(ticker).alias("Ticker"))
        df = df.rename(
            {
                "Return on Assets %\xa0": "ROA",
                "Return on Capital %\xa0": "ROC",
                "Return On Equity %\xa0": "ROE",
                "Gross Profit Margin %\xa0": "Gross Margin",
                "Net Income Margin %\xa0": "Net Margin",
                "Total Debt / Equity": "Debt/Equity",
                "Total Liabilities / Total Assets\xa0": "Liabilities/Assets",
                "Current Ratio\xa0": "Current Ratio",
                "Quick Ratio\xa0": "Quick Ratio",
            }
        )
        df = df.select(
            "Date",
            "Ticker",
            "ROE",
            "ROA",
            "ROC",
            "Gross Margin",
            "Net Margin",
            "Debt/Equity",
            "Liabilities/Assets",
            pl.col("Current Ratio").str.replace("x","").str.replace("\)","").str.replace("\(","-").cast(pl.Float32), 
            pl.col("Quick Ratio").str.replace("x","").str.replace("\)","").str.replace("\(","-").cast(pl.Float32),  
        )
        Ratios.append(df)

df_multiples = pl.concat(Multiples)
df_ratios = pl.concat(Ratios)

# Create Price Collection

# Merge dataframes

# Calculate returns

# Select metric

# Scale metric

# Prep data for OLS print 

# Plot data

# Plot model

