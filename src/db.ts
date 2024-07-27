import sqlite3 from "sqlite3";
import { GQLTransactionData } from "./types";

// Open a database connection
const db = new sqlite3.Database("data.db", (err) => {
  if (err) {
    console.error(err.message);
  }
  console.log("Connected to the SQLite database.");
});

// Prepare the checkColumnStmt statement with a placeholder
const checkColumnStmt = db.prepare(`
  SELECT 1 FROM pragma_table_info('transactions') WHERE name = ?
`);

// Function to check if a column exists
const checkIfColumnExists = (columnName: string) => {
  return new Promise((resolve, reject) => {
    checkColumnStmt.get(columnName, (err, row) => {
      if (err) {
        reject(err);
      } else {
        resolve(row !== undefined);
      }
    });
  });
};

// Example usage

// Create normalized tables
db.exec(`
 CREATE TABLE IF NOT EXISTS transactions (
    id TEXT PRIMARY KEY,
    fee INTEGER NOT NULL,
    size INTEGER NOT NULL,
    owner TEXT,
    block_height INTEGER NOT NULL,
    parent_id TEXT
 )
`);

db.exec(`
 CREATE TABLE IF NOT EXISTS transaction_tags (
   transaction_id TEXT,
   tag_name TEXT,
   tag_value TEXT,
   FOREIGN KEY (transaction_id) REFERENCES transactions(id)
 )
`);

export async function saveTxnData(data: GQLTransactionData[]) {
  const insertStmt = db.prepare(`
        INSERT INTO transactions (id, fee, size, block_height, parent_id, owner)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
        fee = excluded.fee,
        size = excluded.size,
        block_height = excluded.block_height,
        parent_id = excluded.parent_id,
        owner = excluded.owner
    `);

  const insertTagStmt = db.prepare(`
        INSERT INTO transaction_tags (transaction_id, tag_name, tag_value)
        VALUES (?, ?, ?)
    `);

  for (const txn of data) {
    insertStmt.run(txn.id, txn.fee.winston, txn.data.size, txn.block.height, txn.parent?.id, txn.owner.address);

    for (const tag of txn.tags) {
      insertTagStmt.run(txn.id, tag.name, tag.value);
    }
  }
  //   console.log("saved", data.length, "transactions");

  insertStmt.finalize();
  insertTagStmt.finalize();
}

// export async function oldSaveTxnData(data: TransactionData[]) {
//   const addColumnStmt = (column: string) =>
//     db.prepare(`
//     ALTER TABLE transactions ADD COLUMN "${column}" TEXT
//   `);

//   //   const insertStmt = db.prepare(`
//   //     INSERT INTO transactions (id, fee, size, block_height, parent_id)
//   //     VALUES (?, ?, ?, ?, ?)
//   //     ON CONFLICT(id) DO UPDATE SET
//   //       fee = excluded.fee,
//   //       size = excluded.size,
//   //       block_height = excluded.block_height,
//   //       parent_id = excluded.parent_id
//   //   `);

//   for (const txn of data) {
//     console.log("saving", txn.id);

//     const columnsToUpdate: string[] = [];
//     const valuesToUpdate = [txn.id, txn.fee.winston, txn.data.size, txn.block.height, txn.parent?.id];

//     for (let i = 0; i < txn.tags.length; i++) {
//       const tag = txn.tags[i];
//       const tagName = tag.name.toLowerCase();
//       console.log("checking tag", tagName);
//       const columnExists = await checkIfColumnExists(tagName);

//       console.log("columnExists", columnExists);
//       if (!columnExists) {
//         console.log(`Adding column ${tagName}`);
//         const stmt = addColumnStmt(tagName);
//         await new Promise<void>((resolve, reject) => {
//           stmt.run((err) => {
//             if (err) {
//               console.error(`Error adding column ${tagName}:`, err.message);
//               reject(err);
//             } else {
//               stmt.finalize();
//               resolve();
//             }
//           });
//         });
//       }

//       columnsToUpdate.push(`"${tagName}" = ?`);
//       valuesToUpdate.push(tag.value);
//     }

//     const dynamicUpdateStmt = db.prepare(`
//     INSERT INTO transactions (id, fee, size, block_height, parent_id${columnsToUpdate.length ? `, ${columnsToUpdate.map((c) => c.split(" ")[0]).join(", ")}` : ""})
//     VALUES (${valuesToUpdate.map(() => "?").join(", ")})
//     ON CONFLICT(id) DO UPDATE SET
//       fee = excluded.fee,
//       size = excluded.size,
//       block_height = excluded.block_height,
//       parent_id = excluded.parent_id
//       ${columnsToUpdate.length ? `, ${columnsToUpdate.join(", ")}` : ""}
//   `);

//     console.log("upsert txn", txn.id);
//     dynamicUpdateStmt.run(...valuesToUpdate);
//     dynamicUpdateStmt.finalize();
//   }

//   //   insertStmt.finalize();
//   //   checkColumnStmt.finalize();
// }

export default db;

// Insert a user
// db.run(`INSERT INTO users (name) VALUES (?)`, ['Alice'], function(err) {
//     if (err) {
//         console.error(err.message);
//     }
//     console.log(`A row has been inserted with rowid ${this.lastID}`);
// });

// // Query the users
// db.all(`SELECT * FROM users`, [], (err, rows) => {
//     if (err) {
//         throw err;
//     }
//     rows.forEach((row) => {
//         console.log(row);
//     });
// });

// // Close the database connection
// db.close((err) => {
//     if (err) {
//         console.error(err.message);
//     }
//     console.log('Closed the database connection.');
// });
