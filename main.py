# Fetch Libraries
import datetime
import glob
import time
from datetime import date

import matplotlib.pyplot as plt
import polars as pl  # type: ignore
import yfinance as yf
from statsmodels import api as sm  # type: ignore
from tqdm import tqdm

#TODO: Consider logging

#TODO: Make relative metrics
#NTM/LTM P/E & P/S

#NOTE: Consider moving functions to separate /scr folder 

# Timer Function
def timer(func):
    def wrapper(*args):
        t1 = time.time()
        func(*args)
        t2 = round(time.time() - t1, 2)
        print(f"{func.__name__} ran in {t2} seconds\n")

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
@timer
def load_data(path):
    for filename in tqdm(path):
        # Determine the filetype
        name = filename.split("sample data\\")[1].split(".csv")[0]
        if "multiples" in name:
            df = pl.read_csv(filename)
            ticker = name.split("multiples_")[1]
            Stocks.append(ticker)
            df = df.with_columns(pl.lit(ticker).alias("Ticker"))
            df = df.rename(
                {
                    "NTM Revenues\xa0": "NTM Revenues",
                    "NTM Normalized Earnings Per Share\xa0": "NTM EPS",
                    "LTM Diluted EPS Before Extra\xa0": "LTM EPS",
                    "NTM Price / Sales (P/S)": "NTM P/S",
                    "NTM Price / Normalized Earnings (P/E)": "NTM P/E",
                    "LTM Price / Sales (P/S)": "LTM P/S",
                    "LTM Price / Diluted EPS (P/E)\xa0": "LTM P/E",
                }
            )
            df = df.select(
                pl.col("Date").str.to_date("%d/%m/%y"),
                "Ticker",
                pl.col("NTM P/S")
                .str.replace("x", "")
                .str.replace(r"\)", "")
                .str.replace(r"\(", "-")
                .cast(pl.Float32),
                pl.col("NTM P/E")
                .str.replace("x", "")
                .str.replace(r"\)", "")
                .str.replace(r"\(", "-")
                .str.replace(",","")
                .cast(pl.Float32),
                pl.col("LTM P/S")
                .str.replace("x", "")
                .str.replace(r"\)", "")
                .str.replace(r"\(", "-")
                .cast(pl.Float32),
                pl.col("LTM P/E")
                .str.replace("x", "")
                .str.replace(r"\)", "")
                .str.replace(r"\(", "-")
                .str.replace(",","")
                .cast(pl.Float32),
                pl.col("NTM Revenues").cast(pl.Float32),
                pl.col("LTM Revenues").cast(pl.Float32),
                "NTM EPS",
                "LTM EPS",
            )
            Multiples.append(df)
        if "ratios" in name:
            df = pl.read_csv(filename)
            ticker = name.split("ratios_")[1]
            Stocks.append(ticker)
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
                pl.col("Date").str.to_date("%d/%m/%y"),
                "Ticker",
                "ROE",
                "ROA",
                "ROC",
                "Gross Margin",
                "Net Margin",
                "Debt/Equity",
                "Liabilities/Assets",
                pl.col("Current Ratio")
                .str.replace("x", "")
                .str.replace(r"\)", "")
                .str.replace(r"\(", "-")
                .cast(pl.Float32),
                pl.col("Quick Ratio")
                .str.replace("x", "")
                .str.replace(r"\)", "")
                .str.replace(r"\(", "-")
                .cast(pl.Float32),
            )
            Ratios.append(df)
        else:
            ticker = name


load_data(ALL_FILES)
Stocks = list(dict.fromkeys(Stocks))
df_multiples = pl.concat(Multiples)
df_ratios = pl.concat(Ratios)


# Create Price Collection
@timer
def load_prices(list):
    df_dates = pl.date_range(
        date(2013, 12, 1), datetime.date.today(), eager=True
    ).alias("Date")
    df_dates = pl.DataFrame(df_dates)
    for symbol in tqdm(list):
        df_prices = yf.Ticker(symbol).history(start="2013-12-01")
        df_prices = pl.from_pandas(df_prices, include_index=True)
        df_prices = df_prices.with_columns(pl.lit(symbol).alias("Ticker"))
        df_prices = df_prices.select(pl.col("Date").cast(pl.Date), "Ticker", "Close")
        df_price = df_dates.join(df_prices, on="Date", how="left")
        df_price = df_price.select(pl.all().forward_fill())
        Prices.append(df_price)


load_prices(Stocks)
df_close = pl.concat(Prices)

# Merge dataframes
df_fact = df_ratios.join(
    df_multiples, on=["Date", "Ticker"], how="outer_coalesce", suffix="_right"
)

df_fact = df_fact.with_columns(pl.col("Date").dt.offset_by("1y").alias("Date 1Y"))

df_fact = df_fact.join(df_close, on=["Date", "Ticker"], how="left", suffix="_test")

df_fact = df_fact.join(
    df_close,
    left_on=["Date 1Y", "Ticker"],
    right_on=["Date", "Ticker"],
    how="left",
    suffix="_1Y",
)

# Calculate returns
df_fact = df_fact.with_columns(
    (pl.col("NTM Revenues") / pl.col("LTM Revenues")).alias("NTM/LTM Revenues"),
    (pl.col("NTM EPS") / pl.col("LTM EPS")).alias("NTM/LTM EPS"),
    ((pl.col("Close_1Y") - pl.col("Close")) / pl.col("Close")).alias("Return 1Y"),
    (pl.col("NTM P/E") / pl.col("LTM P/E")).alias("NTM/LTM P/E"),
    (pl.col("NTM P/S") / pl.col("LTM P/S")).alias("NTM/LTM P/S")

)

print("""
Select a Metric:
01. ROE             02. ROA             03. ROC
04. Current Ratio   05. Quick Ratio     06. LTM P/S 
07. NTM P/S         08. LTM P/E         09. NTM P/E 
10. Gross Margin    11. Net Margin      12. D/E 
13. Liab./Assets    14. Rel. P/E        15. Rel. P/S
""")

SELECTION_NO = list(range(1,16))
SELECTION_NAME = ['ROE', 'ROA', 'ROC', 'Current Ratio', 'Quick Ratio',
                  'LTM P/S', 'NTM P/S', 'LTM P/E', 'NTM P/E', 'Gross Margin',
                  'Net Margin', 'Debt/Equity', 'Liabilities/Asset', "NTM/LTM P/E", "NTM/LTM P/S"]

selection = input("Metric Selected: ")

try:
    index = int(selection)
    if 1 <= index <= 15:
        selected_metric = SELECTION_NAME[index-1]
    else:
        print("Input is not within the expended range")
        quit()
except ValueError:
    print("Invalid Input, Please enter a valid integer")
    quit()

# Select metric
METRIC = selected_metric


# Prep data for OLS print
df_analyze = df_fact[[f"{METRIC}", "Return 1Y"]].drop_nulls()
metric_mean = df_analyze[f"{METRIC}"].mean()
metric_std = df_analyze[f"{METRIC}"].std()
metric_mask = (df_analyze[f"{METRIC}"] < metric_mean + 2 * metric_std) & (
    df_analyze[f"{METRIC}"] > metric_mean - 2 * metric_std
)
df_analyze = df_analyze.filter(metric_mask)
return_mean = df_analyze["Return 1Y"].mean()
return_std = df_analyze["Return 1Y"].std()
return_mask = (df_analyze["Return 1Y"] < return_mean + 2 * return_std) & (
    df_analyze["Return 1Y"] > return_mean - 2 * return_std
)
df_analyze = df_analyze.filter(return_mask)


# OLS reads numpy structs, not Polars
x = df_analyze[f"{METRIC}"].to_numpy()
y = df_analyze["Return 1Y"].to_numpy()

print(
    f"Return is capped between ( {return_mean - 2*return_std}, {return_mean + 2*return_std} )"
)
print(
    f"{METRIC} is capped between ( {metric_mean - 2*metric_std}, {metric_mean + 2*metric_std} )"
)

# Plot data
model = sm.OLS(y, x).fit()
print(model.summary())

# Plot model
pred_ols = model.get_prediction()
iv_l = pred_ols.summary_frame()["obs_ci_lower"]
iv_u = pred_ols.summary_frame()["obs_ci_upper"]

# Ignore type errors in plot
fig, ax = plt.subplots()
ax.plot(x, y, "o", label="data")  # type: ignore
ax.plot(x, model.fittedvalues, "r--.", label="OLS")  # type: ignore
ax.plot(x, iv_u, "r--")  # type: ignore
ax.plot(x, iv_l, "r--")  # type: ignore
ax.set_ylim([-1, 1])  # type: ignore
ax.legend(loc="best")  # type: ignore
plt.show()
