// src/etl-square-locations.ts
import dotenv from "dotenv";
import { pool, closePool } from "./db.js";
import {
  fetchLocations,
  SquareLocation,
} from "./square.js";

dotenv.config();

const TENANT_ID = process.env.TENANT_ID;
const POS_PROVIDER = process.env.POS_PROVIDER || "square";
const POS_PROVIDER_ACCOUNT_ID =
  process.env.POS_PROVIDER_ACCOUNT_ID || "default-square";

if (!TENANT_ID) {
  throw new Error("TENANT_ID is not set");
}

interface LocationRow {
  location_id: string;
  location_name: string;
  address: string | null;
  timezone: string | null;
  status: string | null;
  raw_payload: string;
}

function mapLocationToRow(location: SquareLocation): LocationRow | null {
  if (!location.id || !location.name) {
    console.warn("Location without id or name – skipping");
    return null;
  }

  // Build address string
  const addr = location.address;
  const addressParts = [
    addr?.address_line_1,
    addr?.locality,
    addr?.administrative_district_level_1,
    addr?.postal_code,
  ].filter(Boolean);
  const addressString = addressParts.length > 0 ? addressParts.join(", ") : null;

  return {
    location_id: location.id,
    location_name: location.name,
    address: addressString,
    timezone: location.timezone ?? null,
    status: location.status ?? null,
    raw_payload: JSON.stringify(location),
  };
}

async function upsertLocationRows(rows: LocationRow[]) {
  if (rows.length === 0) {
    console.log("No location rows to upsert.");
    return;
  }

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    const text = `
      INSERT INTO pos.pos_locations (
        tenant_id,
        provider,
        provider_account_id,
        location_id,
        location_name,
        address,
        timezone,
        status,
        raw_payload
      )
      VALUES (
        $1, $2, $3,
        $4, $5, $6,
        $7, $8, $9
      )
      ON CONFLICT (tenant_id, provider, provider_account_id, location_id)
      DO UPDATE SET
        location_name = EXCLUDED.location_name,
        address       = EXCLUDED.address,
        timezone      = EXCLUDED.timezone,
        status        = EXCLUDED.status,
        raw_payload   = EXCLUDED.raw_payload,
        updated_at    = CURRENT_TIMESTAMP;
    `;

    for (const row of rows) {
      const values = [
        TENANT_ID,
        POS_PROVIDER,
        POS_PROVIDER_ACCOUNT_ID,
        row.location_id,
        row.location_name,
        row.address,
        row.timezone,
        row.status,
        row.raw_payload,
      ];
      await client.query(text, values);
    }

    await client.query("COMMIT");
    console.log(`Upserted ${rows.length} location rows.`);
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}

async function main() {
  console.log("Fetching locations from Square…");
  const locations: SquareLocation[] = await fetchLocations();
  console.log(`Fetched ${locations.length} locations from Square.`);

  const rows: LocationRow[] = [];

  for (const location of locations) {
    const row = mapLocationToRow(location);
    if (row) {
      rows.push(row);
    }
  }

  console.log(`Prepared ${rows.length} location rows to upsert…`);
  await upsertLocationRows(rows);
}

main()
  .then(() => {
    console.log("Locations ETL done.");
    return closePool();
  })
  .catch((err) => {
    console.error("Locations ETL failed:", err);
    return closePool().finally(() => {
      process.exit(1);
    });
  });