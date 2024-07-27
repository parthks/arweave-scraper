// downloaded all data from indexed.xyz. Now uploading to clickhouse
// GQL is very slow and rate limited. So indexed.xyz is a lifesaver

// import { uploadTxnsToClickhouse } from "./clickhouse";
import { uploadTxnsToMeiliSearch } from "./meili";

const fs = require("fs");
const path = require("path");
const fg = require("fast-glob");

const BATCH_SIZE = 100000; // Adjust the batch size based on your memory constraints

export type BatchData = {
  id: string;
  created_at: string;
  data_size: string;
  height: number;
  content_type: string;
  reward: string;
  parent: string;
  owner_address: string;
  tags: { name: string; value: string }[];
};

export type TransactionData = {
  transaction_id: string;
  transaction_date: string;
  size: number;
  block_height: number;
  content_type: string;
  fee: number;
  parent_id: string;
  owner: string;
};

let ALL_TXN_IDS: string[] = [];
if (fs.existsSync("all_txn_ids.txt")) {
  const data = fs.readFileSync("all_txn_ids.txt", "utf8");
  ALL_TXN_IDS = data.split("\n");
  console.log(ALL_TXN_IDS.length, "txn ids loaded from file");
}

console.log("All txn ids:", ALL_TXN_IDS.length);

const uploadData = async (data: BatchData[]) => {
  // await uploadTxnsToClickhouse(data);
  await uploadTxnsToMeiliSearch(data);
  // save the ids to a file
  console.log("Saving txn ids to file");
  const ids = data.map((d) => d.id);
  ALL_TXN_IDS.push(...ids);
  fs.writeFileSync("all_txn_ids.txt", ALL_TXN_IDS.join("\n"));
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
      if (ALL_TXN_IDS.indexOf(data[0]?.id) === -1) {
        console.log("got", data.length, "transactions");
        batch.push(...data);
      }

      // const size = Buffer.byteLength(JSON.stringify(data));
      // console.log("Size of data:", size, "bytes", "Batch size:", batch.length);
      // // if size is greater than 50MB, upload the data
      // if (size > 50 * 1024 * 1024) {
      //   await uploadData(batch);
      //   // Clear the batch
      //   batch.length = 0;
      // }

      if (batch.length > BATCH_SIZE) {
        await uploadData(batch);
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
  const q = `SELECT * FROM read_parquet('${parquetFilePath}')`;

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
