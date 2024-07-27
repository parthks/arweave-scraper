import { MeiliSearch } from "meilisearch";
import { BatchData, TransactionData } from "./main";
import dotenv from "dotenv";
dotenv.config();

const client = new MeiliSearch({
  host: "http://167.172.76.43",
  apiKey: process.env.MEILI_API_KEY,
});

if (!process.env.MEILI_API_KEY) {
  throw new Error("MEILI_API_KEY is not set");
}

const index = client.index("transactions");

export const uploadTxnsToMeiliSearch = async (batch: BatchData[]) => {
  console.log("Uploading batch of", batch.length, "records to MeiliSearch");

  //   waiting for task to be completed
  let tasks = await index.getTasks({ limit: 1, statuses: ["enqueued", "processing"] });
  while (tasks.results.length > 2) {
    console.log("Waiting for previous task to complete");
    await new Promise((resolve) => setTimeout(resolve, 10000));
    tasks = await index.getTasks({ limit: 3, statuses: ["enqueued", "processing"] });
  }

  const readyUploadData = batch.map((data) => {
    return {
      transaction_id: data.id,
      transaction_date: Date.parse(data.created_at),
      size: Number(data.data_size),
      block_height: data.height,
      //   content_type: data.content_type,
      fee: parseInt(data.reward),
      parent_id: data.parent,
      owner: data.owner_address,
      tag_names: data.tags.map((tag) => tag.name),
      //   tags: data.tags,
      tags_obj: data.tags.reduce((acc, tag) => {
        acc[tag.name] = tag.value;
        return acc;
      }, {} as Record<string, string>),
    };
  });
  await index.addDocuments(readyUploadData);
};
