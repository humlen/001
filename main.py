# Fetch Libraries
import datetime
import glob

import matplotlib.pyplot as plt
import pandas as pd
import yfinance as yf
from statsmodels import api as sm
from tqdm import tqdm

#TODO: Make timer function
#TODO: Rewrite portions to functions and call them

# Local data library
FOLDER_PATH = (
    r"C:\Users\eirik\Codebase\001 Compounding Linear Relationships\data\sample data"
)
all_files = glob.glob(FOLDER_PATH + "/*.csv")

# Prepare structures
Ratios = []
Multiples = []
Stocks = []
Prices = []

# Put data in dfs
for filename in tqdm(all_files):
    # Determine what kind of file it is
    name = filename.split("sample data\\")[1].split(".csv")[0]
    if "ratios" in name:
        df = pd.read_csv(filename, index_col=None, header=0)
        ticker = name.split("ratios_")[1]
        df["Ticker"] = ticker
        Ratios.append(df)
    if "multiples" in name:
        df = pd.read_csv(filename, index_col=None, header=0)
        ticker = name.split("multiples_")[1]
        df["Ticker"] = ticker
        Multiples.append(df)
        Stocks.append(ticker)
    else:
        ticker = name
    Stocks.append(ticker)

df_ratios = pd.concat(Ratios, axis=0, ignore_index=True)
df_multiples = pd.concat(Multiples, axis=0, ignore_index=True)

# Create date table
df_dates = pd.DataFrame(
    pd.date_range(start="2013-12-01", end=datetime.datetime.today(), freq="D"),
    columns=["Date"],
)

# Format dfs to raw nums
r = [c for c in df_ratios.columns if c not in ["Ticker"]]
df_ratios[r] = (
    df_ratios[r]
    .replace("x", "", regex=True)
    .replace(r"\)", "", regex=True)
    .replace(r"\(", "-", regex=True)
)

# BUG: Highlighting thinks parentheses within strings are counted
m = [c for c in df_multiples.columns if c not in ["Ticker"]]
df_multiples[m] = (
    df_multiples[m]
    .replace("x", "", regex=True)
    .replace(r"\)", "", regex=True)
    .replace(r"\(", "-", regex=True)
)

# Date formatting
df_ratios["Date"] = pd.to_datetime(df_ratios["Date"], format="%d/%m/%y")
df_ratios["Date 1Y"] = df_ratios["Date"] + datetime.timedelta(days=365)
df_multiples["Date"] = pd.to_datetime(df_multiples["Date"], format="%d/%m/%y")
df_multiples["Date 1Y"] = df_multiples["Date"] + datetime.timedelta(days=365)

# Import stock prices
for symbol in tqdm(Stocks):
    df_price = yf.Ticker(symbol).history(start="2013-12-01")
    df_price["Date"] = df_price.index
    df_price = df_price.reset_index(drop=True)
    try:
        df_price["Date"] = df_price["Date"].dt.tz_convert(None).dt.round("D")
    except:
        pass
    df_price["Ticker"] = symbol
    df_price = df_price[["Date", "Ticker", "Close"]]
    df_price = pd.merge(
        df_dates,
        df_price,
        how="left",
        left_on=["Date"],
        right_on=["Date"],
        suffixes=("", ""),
    )

    # Fill down missing values
    df_price["Close"] = df_price["Close"].ffill()
    df_price["Ticker"] = df_price["Ticker"].ffill()
    Prices.append(df_price)

df_prices = pd.concat(Prices, axis=0)

# Merge dataframes
df_fact = pd.merge(
    df_ratios,
    df_multiples,
    how="outer",  # no particular reason why left, mby inner?
    left_on=["Date", "Ticker"],
    right_on=["Date", "Ticker"],
    suffixes=("", "_RATIOS"),
)

df_fact = pd.merge(
    df_fact,
    df_prices,
    how="left",
    left_on=["Date", "Ticker"],
    right_on=["Date", "Ticker"],
    suffixes=("", ""),
)

df_fact = pd.merge(
    df_fact,
    df_prices,
    how="left",
    left_on=["Date 1Y", "Ticker"],
    right_on=["Date", "Ticker"],
    suffixes=("", "_1Y"),
)

# Fix, rename and remove columns
df_fact = df_fact.rename(
    columns={
        # Ratios
        "Return on Assets %\xa0": "ROA",
        "Return on Capital %\xa0": "ROC",
        "Return On Equity %\xa0": "ROE",
        "Gross Profit Margin %\xa0": "Gross Margin",
        "Net Income Margin %\xa0": "Net Margin",
        "Total Debt / Equity": "Debt/Equity",
        "Total Liabilities / Total Assets\xa0": "Liabilities/Assets",
        "Current Ratio\xa0": "Current Ratio",
        "Quick Ratio\xa0": "Quick Ratio",
        # Multiples
        "NTM Revenues\xa0": "NTM Revenues",
        "NTM Normalized Earnings Per Share\xa0": "NTM EPS",
        "LTM Diluted EPS Before Extra\xa0": "LTM EPS",
        "NTM Price / Sales (P/S)": "NTM P/S",
        "NTM Price / Normalized Earnings (P/E)": "NTM P/E",
        "LTM Price / Sales (P/S)": "LTM P/S",
        "LTM Price / Diluted EPS (P/E)\xa0": "LTM P/E",
    }
)

df_fact["NTM/LTM Revenue"] = df_fact["NTM Revenues"] / df_fact["LTM Revenues"]
df_fact["NTM/LTM EPS"] = df_fact["NTM EPS"] / df_fact["LTM EPS"]
df_fact["Return 1Y"] = (df_fact["Close_1Y"] - df_fact["Close"]) / df_fact["Close"] * 100

df_fact = df_fact[
    [
        "Date",
        "Ticker",
        "Return 1Y",
        "ROE",
        "ROA",
        "ROC",
        "Gross Margin",
        "Net Margin",
        "Debt/Equity",
        "Liabilities/Assets",
        "Current Ratio",
        "Quick Ratio",
        "NTM P/S",
        "NTM P/E",
        "LTM P/S",
        "LTM P/E",
        "NTM Revenues",
        "LTM Revenues",
        "NTM EPS",
        "LTM EPS",
        "NTM/LTM Revenue",
        "NTM/LTM EPS",
    ]
]

# Select metric
METRIC = "ROE"

# TODO: Scale metric

# Prep data for OLS print
df_fact = df_fact[df_fact[f"{METRIC}"] > 0]
df_fact = df_fact[df_fact[f"{METRIC}"] < 1]
df_fact = df_fact.dropna(subset=[f"{METRIC}", "Return 1Y"])
x = df_fact[f"{METRIC}"]
y = df_fact["Return 1Y"]

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
ax.legend(loc="best")  # type: ignore
plt.show()
