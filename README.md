```markdown
# 🎨 Arweave Scraper 🚀

```ascii
                                     _.--""--._
                                   .'          `.
                                  /   O      O   \
                                 |    \  ^^  /    |
                                 \     `----'     /
                                  `. _______ .'
                                    //_____\\
                                   (( ____ ))
                                    `-----'
                               Arweave Blockchain Analytics
```

[![GitHub stars](https://img.shields.io/github/stars/parthks/arweave-scraper?style=for-the-badge)](https://github.com/parthks/arweave-scraper/stargazers)
[![GitHub forks](https://img.shields.io/github/forks/parthks/arweave-scraper?style=for-the-badge)](https://github.com/parthks/arweave-scraper/network)
[![GitHub license](https://img.shields.io/github/license/parthks/arweave-scraper?style=for-the-badge)](https://github.com/parthks/arweave-scraper/blob/main/LICENSE)
[![TypeScript](https://img.shields.io/badge/typescript-%23007ACC.svg?style=for-the-badge&logo=typescript&logoColor=white)](https://www.typescriptlang.org/)
[![Node.js](https://img.shields.io/badge/node.js-6DA55F?style=for-the-badge&logo=node.js&logoColor=white)](https://nodejs.org/)
[![Python](https://img.shields.io/badge/python-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![Google Cloud](https://img.shields.io/badge/google--cloud-%234285f4.svg?style=for-the-badge&logo=google-cloud&logoColor=white)](https://cloud.google.com/)


---

## 🌟 Feature Highlights ✨

-   **Live GraphQL Scraping:**  ⚡ Scrape transaction metadata in real-time using GraphQL.  Handles rate limits and gateway failures intelligently.
-   **Bulk Data Ingestion:** 📦 Efficiently processes large `.parquet` datasets (e.g., from indexed.xyz).
-   **Multi-Database Support:** 📊 Integrates with SQLite, ClickHouse, and MeiliSearch for diverse analytical needs.
-   **Resilient Design:** 🛠️ Robust error handling and gateway rotation ensures continuous operation.
-   **Extensible Architecture:** 💻 Easily add new data sources and output destinations thanks to the TypeScript structure.
-   **Typo-Tolerant Search:** 💡 MeiliSearch enables lightning-fast searches, even with typos.


---

## 🛠️ Tech Stack 🛠️

| Technology        | Badge                                                                   |
|--------------------|------------------------------------------------------------------------|
| Node.js           | [![Node.js](https://img.shields.io/badge/node.js-6DA55F?style=flat-square&logo=node.js&logoColor=white)](https://nodejs.org/) |
| TypeScript        | [![TypeScript](https://img.shields.io/badge/typescript-%23007ACC.svg?style=flat-square&logo=typescript&logoColor=white)](https://www.typescriptlang.org/) |
| SQLite            | [![SQLite](https://img.shields.io/badge/sqlite-%2307405e.svg?style=flat-square&logo=sqlite&logoColor=white)](https://www.sqlite.org/) |
| ClickHouse        | [![ClickHouse](https://img.shields.io/badge/clickhouse-%23007ACC.svg?style=flat-square&logo=clickhouse&logoColor=white)](https://clickhouse.com/) |
| MeiliSearch       | [![MeiliSearch](https://img.shields.io/badge/meilisearch-%23007ACC.svg?style=flat-square&logo=meilisearch&logoColor=white)](https://www.meilisearch.com/) |
| Axios             | [![Axios](https://img.shields.io/badge/axios-%23007ACC.svg?style=flat-square&logo=axios&logoColor=white)](https://axios-http.com/) |
| DuckDB            | [![DuckDB](https://img.shields.io/badge/duckdb-%23007ACC.svg?style=flat-square&logo=duckdb&logoColor=white)](https://duckdb.org/) |
| ParquetJS         | [![ParquetJS](https://img.shields.io/badge/parquetjs-%23007ACC.svg?style=flat-square&logo=parquet&logoColor=white)](https://github.com/apache/parquet-format) |
| Python           | [![Python](https://img.shields.io/badge/python-3776AB?style=flat-square&logo=python&logoColor=white)](https://www.python.org/) |


---

## 🚀 Quick Start ⚡

1.  **Clone the repository:** `git clone https://github.com/parthks/arweave-scraper.git`
2.  **Install dependencies:** `yarn install`  (and `pip install requests`)
3.  **Update gateways:** `python endpoints.py` (fetches a fresh list of Arweave gateways)
4.  **Run the scraper:** `yarn gql` (for live scraping) or `yarn upload` (for bulk ingestion).  Configure `.env` with API keys.


---

## 📖 Detailed Usage 📚

### Live Scraping (GraphQL)

```bash
yarn gql
```

This starts the live scraper.  The starting block, increment, and concurrency are configurable in `src/index.ts`.

### Bulk Data Ingestion (Parquet)

1.  Place your `.parquet` files in a directory (e.g., `/path/to/your/data`).
2.  Update `folderPath` in `src/main.ts` to point to your data directory.
3.  Run: `yarn upload`


---

## 🏗️ Project Structure 📦

```
arweave-scraper/
├── src/                  # TypeScript source code
│   ├── index.ts          # Live scraping entry point
│   ├── main.ts           # Bulk ingestion entry point
│   ├── ...               # Other modules
├── endpoints.py          # Python script for gateway updates
├── gateways.json         # List of Arweave gateways
├── package.json          # Project dependencies
└── tsconfig.json         # TypeScript compiler config
```


---

## 🎯 API Documentation 📖

<details><summary><b>ClickHouse API</b></summary>

| Function             | Description                                                                                                    |
|----------------------|----------------------------------------------------------------------------------------------------------------|
| `uploadTxnsToClickHouse(batch: BatchData[])` | Uploads a batch of transaction data to ClickHouse.                                                        |

</details>

<details><summary><b>MeiliSearch API</b></summary>

| Function             | Description                                                                                                |
|----------------------|------------------------------------------------------------------------------------------------------------|
| `uploadTxnsToMeiliSearch(batch: BatchData[])` | Uploads a batch of transaction data to MeiliSearch.                                                     |

</details>

---

## 🔧 Configuration Options ⚙️

| Variable           | Description                                      | Type    | Default |
|--------------------|--------------------------------------------------|---------|---------|
| `MEILI_API_KEY`     | MeiliSearch API key                             | string  |         |
| `CLICKHOUSE_USERNAME` | ClickHouse username                              | string  |         |
| `CLICKHOUSE_PASSWORD` | ClickHouse password                              | string  |         |
| `BLOCK_MIN`       | Starting block for GraphQL scraper              | number  | 0       |
| `BLOCK_INCREMENT`   | Block increment for GraphQL scraper              | number  | 100     |
| `folderPath`       | Path to Parquet files for bulk ingestion      | string  |         |


---

## 📸 Screenshots/Demo 📸

**(Add screenshots or GIFs here showcasing the application's functionality)**

---

## 🤝 Contributing Guidelines 🌟

1.  Fork the repository.
2.  Create a new branch (`git checkout -b feature/your-feature`).
3.  Make your changes.
4.  Commit your changes (`git commit -m "Add your feature"`).
5.  Push to your branch (`git push origin feature/your-feature`).
6.  Create a pull request.

---

## 📜 License & Acknowledgments 🙏

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 👥 Contributors 🧑‍💻

**(Add contributor avatars and links here)**

---

## 📞 Support & Contact 📧

[![Twitter](https://img.shields.io/badge/twitter-%231DA1F2.svg?style=for-the-badge&logo=twitter&logoColor=white)](https://twitter.com/parthks)
[![Email](https://img.shields.io/badge/email-D7D7D7?style=for-the-badge&logo=gmail&logoColor=white)](mailto:parthks@example.com)


```mermaid
graph TD
    A[Start] --> B{Live Scraping?};
    B -- Yes --> C[GraphQL Scraper];
    B -- No --> D[Bulk Ingestion];
    C --> E[Save to SQLite];
    D --> F[Process .parquet];
    F --> G{ClickHouse?};
    G -- Yes --> H[Upload to ClickHouse];
    G -- No --> I[Upload to MeiliSearch];
    H --> J[Analytics];
    I --> J;
    J --> K[End];
```
