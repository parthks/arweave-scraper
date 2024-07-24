import fs from "fs";

const available_gateways = JSON.parse(fs.readFileSync("gateways.json", "utf8")) as string[];
const ALL_GATEWAYS = JSON.parse(fs.readFileSync("gateways.json", "utf8")) as string[];
console.log("read", available_gateways.length, "gateways");

const rate_limited_gateways = {} as Record<
  string,
  {
    last_request_time: Date;
  }
>;

export default class Gateway {
  endpoint: string;

  static available_gateways = available_gateways;

  constructor() {
    // take a random gateway and remove from the list
    const index = Math.floor(Math.random() * available_gateways.length);
    this.endpoint = available_gateways[index];
    available_gateways.splice(index, 1);
    // console.log("Gateway", this.endpoint, "created");
  }

  addGateWayRateLimited() {
    rate_limited_gateways[this.endpoint] = {
      last_request_time: new Date(),
    };
    this.switchToNextGateway();
  }

  // remove from ALL_GATEWAYS due to 503 error
  removeGatewayFromList() {
    const index = available_gateways.indexOf(this.endpoint);
    if (index !== -1) {
      available_gateways.splice(index, 1);
    }
    const index2 = ALL_GATEWAYS.indexOf(this.endpoint);
    if (index2 !== -1) {
      ALL_GATEWAYS.splice(index2, 1);
    }
  }

  switchToNextGateway() {
    if (available_gateways.length === 0) {
      // remove rate limited gateways if last_request_time is greater than 15 min
      const now = new Date();
      for (const g in rate_limited_gateways) {
        if (now.getTime() - rate_limited_gateways[g].last_request_time.getTime() > 15 * 60 * 1000) {
          delete rate_limited_gateways[g];
        }
      }
      available_gateways.push(...ALL_GATEWAYS.filter((g) => !rate_limited_gateways[g]));
      console.log("Resetting all gateways. available_gateways", available_gateways.length);
      if (available_gateways.length === 0) {
        console.log("No available gateways. Sleeping for 15 min");
        // sleep 15 min
        new Promise((resolve) => setTimeout(resolve, 15 * 60 * 1000));
        // remove all rate limited gateways
        for (const g in rate_limited_gateways) {
          delete rate_limited_gateways[g];
        }
        available_gateways.push(...ALL_GATEWAYS);
      }
    }
    const index = Math.floor(Math.random() * available_gateways.length);
    this.endpoint = available_gateways[index];
    available_gateways.splice(index, 1);
    console.log("Switched to", this.endpoint, "remaining", available_gateways.length, "rate_limited", Object.keys(rate_limited_gateways).length);
  }
}
