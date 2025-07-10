Of course! Here is a comprehensive README for your `arweave-scraper` project, generated based on the file structure and code you provided.

---

# Arweave Scraper

A comprehensive toolkit for scraping and processing analytics data from the Arweave blockchain. It provides tools for both live scraping via GraphQL and bulk ingesting of pre-downloaded transaction data into powerful analytics backends.

## Overview

This project is designed to tackle Arweave data analytics in two primary ways:

1.  **Live GraphQL Scraping (`yarn gql`):** A resilient, concurrent scraper that connects to public Arweave gateways to fetch transaction metadata block-by-block. It's built to handle network issues, rate limits, and failing gateways by intelligently rotating through a large, fresh list of endpoints. Data from this process is saved to a local SQLite database (`data.db`).

2.  **Bulk Data Ingestion (`yarn upload`):** A high-performance script designed to process large, pre-downloaded datasets of Arweave transactions in `.parquet` format (such as those from [indexed.xyz](https://indexed.xyz/)). It uses DuckDB for efficient in-memory processing and uploads the cleaned, structured data into **MeiliSearch** for fast, typo-tolerant search and **ClickHouse** for large-scale analytical queries.

## Features

-   **Resilient Gateway Management:** Automatically rotates through a large pool of public Arweave gateways, handling rate limits (429), timeouts, and permanent failures (5xx) to ensure continuous scraping.
-   **Dual Data Ingestion Modes:**
    -   **Live Scraper:** Fetches real-time transaction data via GraphQL.
    -   **Bulk Ingestor:** Processes large `.parquet` datasets efficiently using DuckDB.
-   **Multiple Database Backends:** Supports storing and searching data in:
    -   **SQLite:** For local development and smaller-scale scraping.
    -   **ClickHouse:** For storing massive datasets for complex analytical queries.
    -   **MeiliSearch:** For providing a fast, full-text search experience over transaction metadata and tags.
-   **Extensible:** Built with TypeScript, making it easy to add new data sources or destinations.

## Getting Started

Follow these instructions to get a copy of the project up and running on your local machine.

### Prerequisites

-   [Node.js](https://nodejs.org/) (v20.x or higher recommended)
-   [Yarn](https://yarnpkg.com/)
-   [Python 3](https://www.python.org/downloads/)
-   `pip` (Python package installer)

### Installation

1.  **Clone the repository:**
    ```sh
    git clone https://github.com/your-username/arweave-scraper.git
    cd arweave-scraper
    ```

2.  **Install Node.js dependencies:**
    ```sh
    yarn install
    ```

3.  **Install Python dependencies:**
    ```sh
    pip install requests
    ```

4.  **Set up environment variables:**
    Create a `.env` file in the root of the project and add the necessary credentials for the services you intend to use.

    ```env
    # MeiliSearch Credentials (for bulk upload)
    MEILI_API_KEY=your_meilisearch_api_key

    # Clickhouse Credentials (for bulk upload)
    CLICKHOUSE_USERNAME=your_clickhouse_username
    CLICKHOUSE_PASSWORD=your_clickhouse_password
    ```

## Usage

### 1. Update Gateway List (Recommended First Step)

Before scraping, it's a good idea to get a fresh list of active Arweave gateways.

```sh
python endpoints.py
```

This script queries the Viewblock API, filters for healthy HTTPS gateways, and saves them to `gateways.json`.

### 2. Live Scraping (GraphQL)

This script scrapes data block-by-block using GraphQL and saves it to a local `data.db` SQLite file. It is designed to run multiple concurrent workers for faster scraping.

```sh
yarn gql
```

You can configure the starting block, block increment, and concurrency in `src/index.ts`.

### 3. Bulk Data Ingestion (Parquet Files)

This script processes `.parquet` files and uploads them to MeiliSearch and/or ClickHouse. It is optimized for handling the data dumps from [indexed.xyz](https://indexed.xyz/).

1.  Place your `.parquet` files in a directory on your local machine.
2.  Update the `folderPath` variable in `src/main.ts` to point to this directory:
    ```typescript
    // src/main.ts
    const folderPath = "/path/to/your/arweave/data/transactions";
    ```
3.  Run the upload script:
    ```sh
    yarn upload
    ```

*Note: The script currently prioritizes uploading to MeiliSearch. The ClickHouse upload functionality is present in `src/clickhouse.ts` but is commented out in `src/main.ts`. You can easily re-enable it if needed.*

### Available Scripts

-   `yarn gql`: Run the live GraphQL scraper.
-   `yarn upload`: Run the bulk data ingestor for Parquet files.
-   `yarn build`: Compile TypeScript code to JavaScript in the `dist/` directory.
-   `yarn start`: Run the compiled version of the live scraper from the `dist/` directory.

## Project Structure

```
arweave-scraper/
├── src/                  # Main TypeScript source code
│   ├── index.ts          # Entry point for live GraphQL scraping
│   ├── main.ts           # Entry point for bulk Parquet data ingestion
│   ├── graphql.ts        # Arweave GraphQL queries and client logic
│   ├── Gateway.ts        # Manages the pool of Arweave gateways for resilience
│   ├── db.ts             # SQLite database handler
│   ├── clickhouse.ts     # ClickHouse database handler
│   ├── meili.ts          # MeiliSearch handler
│   └── types.ts          # TypeScript type definitions
├── endpoints.py          # Python script to fetch an up-to-date gateway list
├── gateways.json         # Stores the list of Arweave gateways
├── package.json          # Project dependencies and scripts
└── tsconfig.json         # TypeScript compiler configuration
```
