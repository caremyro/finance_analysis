# Finance Analysis Project

## Overview
This project is a comprehensive data analysis of diverse financial instruments. The objective is to leverage Python and Data Science libraries to transform raw historical prices into actionable financial insights.

I aim to answer key investment questions such as: 
* **Volatility Analysis:** Which asset is the most volatile and carries the highest risk?
* **Crisis Resilience:** How do these assets behave during "Black Swan" events or market disasters? 
* **Market Correlation:** Are there significant correlations between traditional stocks, indices, and cryptocurrencies?

To achieve this, I developed a pipeline to **Retrieve**, **Transform**, and **Visualize** data for a balanced portfolio: **CAC40, S&P 500, Tesla, Apple, and Bitcoin.**

---

## 1. Data Retrieval Strategy

The first step of the pipeline involves fetching historical market data. For this project, I chose a mix of equity, indices, and crypto-assets to observe different market behaviors.

### Source & Storage
* **Data Source:** Financial APIs (e.g., Yahoo Finance) providing adjusted closing prices to account for dividends and stock splits.
* **Storage:** Each asset's raw data is exported into a dedicated CSV file for persistence and offline analysis.
* **Period:** Historical data is retrieved with a daily frequency to ensure high-precision calculations.

### Raw Data Structure
Before any transformation, the raw datasets are standardized in the following format:

| Date | AAPL (Open) | AAPL (High) | AAPL (Low) | AAPL (Close) | AAPL (Volume) |
| :--- | :--- | :--- | :--- | :--- | :--- |
| 2021-02-01 | 130.19 | 131.78 | 127.45 | 130.57 | 106 239 800 |
| 2021-02-02 | 132.12 | 132.69 | 131.03 | 131.40 | 83 305 400 |
| 2021-02-03 | 132.15 | 132.16 | 130.06 | 130.38 | 89 880 900 |

## Transform Data

In this section, I process the raw data to extract technical indicators. These metrics are essential for analyzing trends and risk levels.

### Calculated Indicators
Using Python and the Pandas library, I generate the following metrics for each asset:

### Mathematical Indicators

* **Daily Returns**: The fundamental building block of performance analysis.

$$\text{Daily Return}(R_t) = \frac{P_t - P_{t-1}}{P_{t-1}}$$

* **Moving Average (MA)**: Used to smooth out price "noise" and highlight trends.

$$\text{MA} = \frac{1}{n} \sum_{i=1}^{n} P_{i}$$

* **Volatility**: Measures the risk by calculating the standard deviation of returns.

$$\sigma = \sqrt{\frac{1}{N} \sum_{i=1}^{N} (R_i - \bar{R})^2}$$

* **Drawdown**: Quantifies the percentage decline from the historical peak.

$$\text{Drawdown} = \frac{\text{Price} - \text{Peak}}{\text{Peak}}$$

### Transformed Data Sample
Here is a sample of the data after processing (Example: Apple):

| Date | Daily Returns | Moving Average | Volatility | Drawdown |
| :--- | :--- | :--- | :--- | :--- |
| 2021-03-01 | +5.38% | 128.01 | - | -6.85% |
| 2021-03-02 | -2.09% | 127.58 | 0.299 | -8.79% |
| 2021-03-03 | -2.45% | 126.96 | 0.306 | -11.02% |

> **Note:** The initial rows for Moving Average and Volatility contain empty values (NaN) because they require a historical window of data (e.g., 20 days) to be calculated.

## Load: Database Architecture & Storage

Once the data is extracted and the financial indicators are calculated, the final stage of the pipeline persists this information into a **relational database (MySQL)**. This architecture ensures data persistence, integrity, and allows for complex analytical queries that would be inefficient on flat files.

### Relational Database Schema
The database is organized into three distinct tables to optimize storage and maintain clear logical relationships between assets and their metrics:



* **Tickers Table**: The core reference table storing descriptive metadata for each asset (Company name, Sector, Currency, and Exchange). This avoids repeating static information in every row of price data.
* **Stocks Table**: Dedicated to raw historical market data (Open, High, Low, Close, Volume). It uses a composite primary key `(date, ticker_id)` to prevent time-series overlaps.
* **Indicators Table**: A specialized storage for "Financial Intelligence". By separating indicators (Yields, Volatility, Drawdowns) from raw prices, we can update our calculation models without altering the original market history.

### Data Integrity & Loading Logic
The loading process is designed to be robust and reusable, incorporating several industrial-grade safety features:

* **Automated Schema Management**: The pipeline automatically detects if the database and tables exist. If not, it generates the entire infrastructure (DDL) upon the first run.
* **Conflict Prevention**: Using `INSERT IGNORE` logic, the system gracefully handles existing data, allowing for interrupted runs to be resumed without creating duplicate records or crashing.
* **Data Sanitization**: Financial calculations often produce "NaN" (Not a Number) values (e.g., during the first few days of a Moving Average window). Our loader automatically converts these into `NULL` values to maintain database standards.
* **Relational Constraints**: The use of **Foreign Keys** with `ON DELETE CASCADE` ensures that if an asset is removed from the system, all its related history and indicators are cleaned up automatically, preventing "orphan" data.

### Database Schema Visualization

Below is the entity-relationship diagram representing how the financial data is structured:

```mermaid
erDiagram
    TICKERS ||--o{ STOCKS : "(1:N)"
    TICKERS ||--o{ INDICATORS : "(1:N)"

    TICKERS {
        string ticker_id PK
        string name
        string sector
        string currency
        string stock_exchange
    }

    STOCKS {
        datetime date PK
        string ticker_id PK, FK
        double open
        double high
        double low
        double close
        bigint volume
    }

    INDICATORS {
        datetime date PK
        string ticker_id PK, FK
        double daily_yield
        double moving_average
        double volatility
        double drawdown
    }