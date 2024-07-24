import Gateway from "./Gateway";
import { saveTxnData } from "./db";
import { makeTxnGraphqlCall, txnQueryResponse } from "./graphql";

console.log("hello world");

async function main(gate: Gateway, block_min: number = 0, block_max: number = 10) {
  console.log(block_min, "getting transactions");
  let hasNextPage = true;
  let lastCursor = undefined;
  let total_txns_count = 0;
  do {
    const response: txnQueryResponse = await makeTxnGraphqlCall(gate.endpoint, { block_min, block_max, after: lastCursor });
    // console.log("got", response.data.transactions.edges.length, "transactions");

    const txns = response.data.transactions.edges;
    if (txns.length === 0) return total_txns_count;
    total_txns_count += txns.length;
    // need only transactions with quantity is 0
    const dataTransactions = txns.filter((txn) => txn.node.quantity.winston === "0");
    console.log(block_min, "got", dataTransactions.length, "data transactions");
    await saveTxnData(dataTransactions.map((txn) => txn.node));

    hasNextPage = response.data.transactions.pageInfo.hasNextPage;
    console.log(block_min, "next page", hasNextPage);
    lastCursor = txns[txns.length - 1].cursor;
  } while (hasNextPage);
  return total_txns_count;
}

// all blocks below 561800 do not have an owner
const INITIAL_BLOCK_INCREMENT = 5;
let BLOCK_MIN = 611100;
let BLOCK_INCREMENT = INITIAL_BLOCK_INCREMENT;

// 467928 - 468088 - in one minute

async function runLoop() {
  let currentBlock = BLOCK_MIN;
  BLOCK_MIN += BLOCK_INCREMENT;
  const gate = new Gateway();
  while (true) {
    console.log("\nSTARTING BLOCK", currentBlock, "with", gate.endpoint);
    try {
      const total_txn_count = await main(gate, currentBlock, currentBlock + BLOCK_INCREMENT);
      //   if (total_txn_count < 100) {
      //     BLOCK_INCREMENT *= 5;
      //   } else {
      //     BLOCK_INCREMENT = INITIAL_BLOCK_INCREMENT;
      //   }
      gate.switchToNextGateway();

      console.log("\nBLOCK", currentBlock, "-", currentBlock + BLOCK_INCREMENT, "done\n\n");
      // sleep 10 seconds
      await new Promise((resolve) => setTimeout(resolve, 3000));
      currentBlock = BLOCK_MIN;
      BLOCK_MIN += BLOCK_INCREMENT;
    } catch (e) {
      const error = e as Error;
      //   console.error(e);
      console.log(currentBlock, "RETRYING", error);
      if (error.toString().includes("429") || error.toString().includes("timeout of 10000ms")) {
        console.log(currentBlock, "RATE LIMITED GATEWAY", gate.endpoint);
        gate.addGateWayRateLimited();
        await new Promise((resolve) => setTimeout(resolve, 15000));
      } else if (
        error.toString().includes("503") ||
        error.toString().includes("redirects exceeded") ||
        error.toString().includes("certificate has expired") ||
        error.toString().includes("502") ||
        error.toString().includes("ECONNREFUSED") ||
        error.toString().includes("400")
      ) {
        console.log(currentBlock, "GATEWAY 503", gate.endpoint);
        gate.removeGatewayFromList();
      }
      gate.switchToNextGateway();
    }
  }
}

runLoop();
runLoop();
runLoop();
runLoop();
runLoop();
runLoop();
runLoop();
