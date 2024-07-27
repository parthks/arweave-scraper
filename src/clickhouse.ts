import { createClient } from "@clickhouse/client";
import { BatchData, TransactionData } from "./main";

const client = createClient({
  url: "https://q50r0xb4wu.asia-southeast1.gcp.clickhouse.cloud:8443",
  username: process.env.CLICKHOUSE_USERNAME,
  password: process.env.CLICKHOUSE_PASSWORD,
});

export const uploadTxnsToClickhouse = async (batch: BatchData[]) => {
  console.log("Uploading batch of", batch.length, "records to Clickhouse");
  const tags = [] as any[];
  const readyUploadData: TransactionData[] = batch.map((data) => {
    const txnTags = data.tags.map((tag) => {
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
