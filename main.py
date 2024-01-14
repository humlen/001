# Fetch Libraries
import datetime
import glob

import matplotlib.pyplot as plt
import pandas as pd
import statsmodels.api as sm
import yfinance as yf
from tqdm import tqdm

# Import data into dataframes
FOLDER_PATH = (
    r"C:\Users\eirik\Codebase\001 Compounding Linear Relationships\data\sample data"
)
all_files = glob.glob(FOLDER_PATH + "/*.csv")

# Create empty lists
Ratios = []
Stocks = []



# Ratios
for filename in tqdm(all_files):
    name = filename.split("sample data\\")[1].split(".csv")[0]
    if "ratios" in name:
        df = pd.read_csv(filename, index_col=None, header=0)
        ticker = name.split("ratios_")[1]
        df["Ticker"] = ticker
        Ratios.append(df)
        Stocks.append(ticker)
    else:
        pass


# Create a dataframe with dates from 2013-12-01 to today
df_dates = pd.DataFrame(
    pd.date_range(start="2013-12-01", end=datetime.datetime.today(), freq="D"),
    columns=["Date"],
)

df_ratio = pd.concat(Ratios, axis=0, ignore_index=True)

# Remove x from all rows in dataframe
f = [c for c in df_ratio.columns if c not in ["Ticker"]]
df_ratio[f] = (
    df_ratio[f]
    .replace("x", "", regex=True)
    .replace(r"\)", "", regex=True)
    .replace(r"\(", "-", regex=True)
)

# Format dates
df_ratio["Date"] = pd.to_datetime(df_ratio["Date"], format="%d/%m/%y")
df_ratio["Date 1Y"] = df_ratio["Date"] + datetime.timedelta(days=365)

Prices = []

## Create a dataframe with dates from 2013-12-01 to today
df_dates = pd.DataFrame(
    pd.date_range(start="2013-12-01", end=datetime.datetime.today(), freq="D"),
    columns=["Date"],
)

# Import stock prices
for symbol in tqdm(Stocks):
    df_price = yf.Ticker(symbol).history(start="2013-12-01")
    df_price["Date"] = df_price.index
    df_price = df_price.reset_index(drop=True)
    df_price["Date"] = df_price["Date"].dt.tz_convert(None).dt.round("D")
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
df = pd.merge(
    df_ratio,
    df_prices,
    how="left",
    left_on=["Date", "Ticker"],
    right_on=["Date", "Ticker"],
    suffixes=("", ""),
)
df = pd.merge(
    df,
    df_prices,
    how="left",
    left_on=["Date 1Y", "Ticker"],
    right_on=["Date", "Ticker"],
    suffixes=("", "_1Y"),
)

df = df.drop(["Date_1Y"], axis=1)
df = df.rename(columns={"Close_1Y": "Close 1Y"})

# Calculate 1Y return
df["Return 1Y"] = (df["Close 1Y"] - df["Close"]) / df["Close"] * 100

# Rename Return on Assets % Column
df = df.rename(
    columns={
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

# REMOVE IN PROD
print(list(df.columns.values))

# Convert to numeric
df["Current Ratio"] = pd.to_numeric(df["ROA"], errors="coerce")
df["Quick Ratio"] = pd.to_numeric(df["ROC"], errors="coerce")

# Select key columns
df = df[
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
    ]
]

# REMOVE IN PROD
print(df.info())

# Select Metric
METRIC = "ROE"

# Prepare data
df = df[df[f"{METRIC}"] > 0]
df = df[df[f"{METRIC}"] < 1]
df = df.dropna()
x = df[f"{METRIC}"]
y = df["Return 1Y"]


# Plot data
model = sm.OLS(y, x).fit()
print(model.summary())

# Plot model
pred_ols = model.get_prediction()
iv_l = pred_ols.summary_frame()["obs_ci_lower"]
iv_u = pred_ols.summary_frame()["obs_ci_upper"]

fig, ax = plt.subplots(figsize=(8, 6))
ax.plot(x, y, "o", label="data")
ax.plot(x, model.fittedvalues, "r--.", label="OLS")
ax.plot(x, iv_u, "r--")
ax.plot(x, iv_l, "r--")
ax.legend(loc="best")
plt.show()

# TODO: Add multiples
