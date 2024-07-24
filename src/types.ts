export type TransactionData = {
  id: string;
  owner: {
    address: string;
  };
  fee: {
    winston: string;
  };
  quantity: {
    winston: string;
  };
  data: {
    size: number;
    type: string;
  };
  tags: {
    name: string;
    value: string;
  }[];
  block: {
    id: string;
    timestamp: string;
    height: number;
  };
  parent: {
    id: string;
  } | null;
};
