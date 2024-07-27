import axios, { AxiosError } from "axios";
import { GQLTransactionData } from "./types";

// variables are after, block_min, block_max, limit
type makeTxnGraphqlCallVariables = {
  after?: string;
  block_min: number;
  block_max: number;
  limit?: number;
};
export async function makeTxnGraphqlCall(endpoint: string, variables: makeTxnGraphqlCallVariables): Promise<txnQueryResponse> {
  return makeGraphqlCall<txnQueryResponse>(endpoint, txnQuery, variables);
}

// variables are after, limit
export async function makeBlocksGraphqlCall(endpoint: string, variables = {}) {
  return makeGraphqlCall(endpoint, blocksQuery, variables);
}

async function makeGraphqlCall<T>(endpoint: string, query: string, variables: any): Promise<T> {
  const url = endpoint + "/graphql";
  const headers = {
    "Content-Type": "application/json",
  };
  const payload = {
    query: query,
    variables: variables,
  };

  try {
    const response = await axios.post(url, payload, { headers: headers, timeout: 10000 });
    if (response.status === 200) {
      return response.data;
    } else {
      throw new Error(`Query failed with status code ${response.status}: ${response.statusText}`);
    }
  } catch (e) {
    const error = e as AxiosError;

    throw new Error(`Request failed: ${error.message} with status ${error.response?.status} and response ${error.response?.data}`);
  }
}

const blocksQuery = `query($after: String, $limit: Int = 100) {
    blocks(
        after: $after,
        sort: HEIGHT_ASC,
        first: $limit
    ) {
      edges {
        cursor
        node {
            id
            timestamp
            height
            previous  
        }
      }
    }
}`;

export type txnQueryResponse = {
  data: {
    transactions: {
      pageInfo: {
        hasNextPage: boolean;
      };
      edges: {
        cursor: string;
        node: GQLTransactionData;
      }[];
    };
  };
};

const txnQuery = `
query ($after: String, $block_min: Int!, $block_max: Int!, $limit: Int = 100) {
    transactions(
        after: $after,
        sort: HEIGHT_ASC,
        first: $limit,
        block: {min: $block_min, max: $block_max}
        ) {
        pageInfo {
            hasNextPage 
        }
        edges {
            cursor
            node {
                id
                owner {
                    address
                }
                fee {
                    winston
                }
                quantity {
                    winston
                }
                data {
                    size
                    type
                }
                tags {
                    name
                    value
                }
                block {
                    id
                    timestamp
                    height
                }
                parent {
                    id
                }
            }
        }
    }
}
`;
