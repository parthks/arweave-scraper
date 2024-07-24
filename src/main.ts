// downloaded all data from indexed.xyz. Now uploading to clickhouse
// GQL is very slow and rate limited. So indexed.xyz is a lifesaver

const fs = require("fs");
const path = require("path");
const fg = require("fast-glob");
import { createClient } from "@clickhouse/client";
// import { parquetMetadata, parquetRead } from "hyparquet";

// import * as parquet from "@dsnp/parquetjs";

// @ts-ignore
// import * as parquet from "parquetjs-lite";
// import { ParquetSchema, ParquetWriter, ParquetReader } from "parquets";

const client = createClient({
  url: "https://q50r0xb4wu.asia-southeast1.gcp.clickhouse.cloud:8443",
  username: process.env.CLICKHOUSE_USERNAME,
  password: process.env.CLICKHOUSE_PASSWORD,
});

const BATCH_SIZE = 20000; // Adjust the batch size based on your memory constraints

const uploadTxnsToClickhouse = async (batch: any[]) => {
  console.log("Uploading batch to Clickhouse");
  const tags = [] as any[];
  const readyUploadData = batch.map((data) => {
    const txnTags = data.tags.map((tag: { name: string; value: string }[]) => {
      return { transaction_id: data.id, ...tag };
    });
    tags.push(...txnTags);

    return {
      transaction_id: data.id,
      transaction_date: data.created_at,
      size: Number(data.data_size),
      block_height: data.height,
      content_type: data.content_type,
      fee: parseInt(data.reward),
      parent_id: data.parent,
      owner: data.owner_address,
    };
  });

  // Create a session

  //   const createTempTxnTableQuery = `
  //   CREATE TEMPORARY TABLE temp_transactions AS SELECT * FROM transactions WHERE 1 = 0;
  // `;
  //   await client.query({
  //     query: createTempTxnTableQuery,
  //     clickhouse_settings: {
  //       wait_end_of_query: 1,
  //     },
  //   });

  console.log("Uploading", readyUploadData.length, "transactions and", tags.length, "tags");
  const txnRows = await client.insert({
    table: "transactions",
    values: readyUploadData,

    clickhouse_settings: {
      // Allows to insert serialized JS Dates (such as '2023-12-06T10:54:48.000Z')
      date_time_input_format: "best_effort",
    },
    format: "JSONEachRow",
  });
  // Move distinct rows from temp_transactions to transactions
  //   const distinctQuery = `
  //      INSERT INTO transactions
  //      SELECT DISTINCT * FROM temp_transactions;
  //    `;
  //   await client.query({
  //     query: distinctQuery,
  //     clickhouse_settings: {
  //       wait_end_of_query: 1,
  //     },
  //   });

  //   const createTempTagsTableQuery = `
  //   CREATE TEMPORARY TABLE temp_transactions AS SELECT * FROM transactions WHERE 1 = 0;
  // `;
  //   await client.query({
  //     query: createTempTagsTableQuery,
  //     clickhouse_settings: {
  //       wait_end_of_query: 1,
  //     },
  //   });
  const tagRows = await client.insert({
    table: "transaction_tags",
    values: tags,
    clickhouse_settings: {
      // Allows to insert serialized JS Dates (such as '2023-12-06T10:54:48.000Z')
      date_time_input_format: "best_effort",
    },
    format: "JSONEachRow",
  });
  // Move distinct rows from temp_transactions to transactions
  //   const distinctTagsQuery = `
  //     INSERT INTO transaction_tags
  //     SELECT DISTINCT * FROM temp_transactions;
  //   `;
  //   await client.query({
  //     query: distinctTagsQuery,
  //     clickhouse_settings: {
  //       wait_end_of_query: 1,
  //     },
  //   });
  console.log(readyUploadData);
  console.log("Done uploading batch to Clickhouse", tagRows.executed, txnRows.executed);
  //   throw new Error("Error");
};

const getFilesInBatches = async (folderPath: string) => {
  try {
    console.log("Reading files from folder:", folderPath);
    const files: string[] = await fg([`${folderPath}/**/*`], { dot: true });
    console.log("Processing files:", files.length);
    const batch = [];
    let count = 1;
    for (const file of files) {
      count++;
      console.log("Processing file:", file, batch.length, count, "of", files.length);
      let data = [];
      try {
        data = await getFileData(file);
      } catch (e) {
        console.error("Error reading file:", file);
        // store the file path in a separate file
        fs.appendFileSync("error_files.txt", file + "\n");
        continue;
      }
      console.log("got", data.length, "transactions");
      batch.push(...data);
      if (batch.length > BATCH_SIZE) {
        await uploadTxnsToClickhouse(batch);
        // Clear the batch
        batch.length = 0;
      }
    }
  } catch (error) {
    console.error("Error reading files:", error);
  }
};

const duckdb = require("duckdb");

const db = new duckdb.Database(":memory:");
const conn = new duckdb.Connection(db);

async function _convertParquetToJson(parquetFilePath: string): Promise<any[]> {
  //   const schema = new parquet.ParquetSchema({
  //     created_at: { type: "TIMESTAMP_MILLIS" },
  //   });

  //   const reader = await parquet.ParquetReader.openFile(parquetFilePath);
  //   const cursor = reader.getCursor();
  //   const records: any[] = [];

  //   let record = null;
  //   while ((record = await cursor.next())) {
  //     console.log("record", record);
  //     records.push(record);
  //   }

  //   await reader.close();

  const q = `SELECT * FROM read_parquet('${parquetFilePath}')`;
  console.log(q);

  try {
    const stmt = await conn.prepare(q);
    const result = (await new Promise((resolve, reject) => {
      stmt.all((err: any, row: any) => {
        if (err) {
          reject(err);
        } else {
          resolve(row);
        }
      });
    })) as any[];
    // records.push(...result);
    // stmt.finalize();
    // conn.close();
    return result;
  } catch (err) {
    console.error(err);
    throw err;
  } finally {
    // db.close();
  }
}

// [{name=IPFS-Hash, value=bafkreicucxn362mutge6chtjms5p7qs7onqjslcgmf4dswkjf7dfpk65fi}, {name=Content-Type, value=application/json; charset=utf-8}]
// Function to convert the input string to a JSON-compatible string
function _convertToJson(str: string) {
  // Remove the square brackets
  str = str.slice(1, -1);
  const jsonArrayResult = [] as any;

  if (!str) return jsonArrayResult;

  // Split the string into individual objects
  const objects = str.split("}, {");

  // Clean up each object string
  objects.forEach((obj) => {
    // console.log("obj", obj);
    // Split into key-value pairs
    const keyValuePairs = obj.replace(/[{}]/g, "");
    // console.log("keyValuePairs", keyValuePairs);

    const value = keyValuePairs.split("value=")[1].trim();
    const key = keyValuePairs.split("value=")[0].split("name=")[1].trim().slice(0, -1);
    // console.log("key", key, "value", value, "\n");

    // Create a new object with quoted keys and values
    // const jsonKeyValuePairs = keyValuePairs.map((pair) => {
    //   console.log("pair", pair);
    //   const [key, value] = pair.split("=");
    //   return `"${key.trim()}": "${value.trim()}"`;
    // });

    // Join the key-value pairs into a JSON string
    // return "{" + `"${key.trim()}": "${value.trim()}"` + "}";
    jsonArrayResult.push({ name: key, value: value });
  });

  // Join all objects into a JSON array string
  return jsonArrayResult;
}

// open file
async function getFileData(filePath: string) {
  const file = await _convertParquetToJson(filePath);
  //   console.log("GOT FILE", file.length);
  const parsedTags = file
    .filter((f) => f.quantity === "0") // Filter out the currency transfers
    .map((f) => {
      // console.log(f);

      return {
        ...f,
        tags: _convertToJson(f.tags),
      };
    });
  //   console.log(parsedTags);
  return parsedTags;
}

// Set the path to your folder
const folderPath = "/Users/parth/data/arweave/raw/transactions";
getFilesInBatches(folderPath);

// const d = _convertToJsonString(
//   `[{name=Content-Type, value=application/x-bittorrent}, {name=app_name, value=InternetArchive}, {name=id, value=gov.uscourts.ilnb.1526517}, {name=identifier, value=gov.uscourts.ilnb.1526517}, {name=collection, value=usfederalcourts additional_collections}, {name=contributor, value=<a href="https://free.law" rel="nofollow">Free Law Project</a>}, {name=court, value=ilnb}, {name=description, value=This item represents a case in PACER, the U.S. Government's website for federal case data. This information is uploaded quarterly. To see our most recent version please use the source url parameter, linked below. To see the canonical source for this data, please consult PACER directly.}, {name=language, value=eng}, {name=licenseurl, value=https://www.usa.gov/government-works}, {name=mediatype, value=texts}, {name=scanner, value=Internet Archive Python library 1.9.4}, {name=source_url, value=https://www.courtlistener.com/docket/18473780/meechelle-t-jones/}, {name=title, value=Meechelle T. Jones}, {name=uploader, value=recapinfo@gmail.com}, {name=publicdate, value=2020-10-12 18:48:48}, {name=addeddate, value=2020-10-12 18:48:48}]`
// );
// console.log(d);

// read file
// async function main() {
//   //   const d = _convertParquetToJson("/Users/parth/data/arweave/raw/transactions/1680644853-90b71525-fc77-4712-a4ea-d02c3642cd9e-0-1000.parquet");
//   const data = await getFileData("/Users/parth/data/arweave/raw/transactions/1680644853-90b71525-fc77-4712-a4ea-d02c3642cd9e-0-1000.parquet");
//   console.log(data);
//   //   await uploadTxnsToClickhouse(data);
// }
// main();

// ERROR files
// "/Users/parth/data/arweave/raw/transactions/1680738867-352daa7d-4cb4-411e-bd9b-d6d552d0a12e-0-253204.parquet"

// open file /Users/parth/data/arweave/raw/transactions/1680644853-90b71525-fc77-4712-a4ea-d02c3642cd9e-0-1000.parquet"
// const d = fs.readFileSync("/Users/parth/data/arweave/raw/transactions/1680644853-90b71525-fc77-4712-a4ea-d02c3642cd9e-0-1000.parquet");
// // write to file
// fs.writeFileSync("file.txt", d);
// console.log("File has been created");
