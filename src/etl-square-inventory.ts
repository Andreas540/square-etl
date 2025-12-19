// src/etl-square-inventory.ts
import dotenv from "dotenv";
import { pool, closePool } from "./db.js";
import {
  fetchInventoryCounts,
  SquareInventoryCount,
} from "./square.js";

dotenv.config();

const TENANT_ID = process.env.TENANT_ID;
const POS_PROVIDER = process.env.POS_PROVIDER || "square";
const POS_PROVIDER_ACCOUNT_ID =
  process.env.POS_PROVIDER_ACCOUNT_ID || "default-square";

if (!TENANT_ID) {
  throw new Error("TENANT_ID is not set");
}

interface InventoryRow {
  catalog_object_id: string;
  catalog_object_type: string | null;
  location_id: string | null;
  state: string;
  quantity: number;
  calculated_at: Date | null;
  raw_payload: string;
}

function mapInventoryCountToRow(
  count: SquareInventoryCount
): InventoryRow | null {
  if (!count.catalog_object_id) {
    console.warn("Inventory count without catalog_object_id – skipping");
    return null;
  }

  const quantityStr = count.quantity ?? "0";
  const quantity = parseFloat(quantityStr);
  if (!Number.isFinite(quantity)) {
    console.warn(
      `Inventory count for ${count.catalog_object_id} has invalid quantity '${quantityStr}' – skipping.`
    );
    return null;
  }

  const calculatedAt = count.calculated_at
    ? new Date(count.calculated_at)
    : null;

  return {
    catalog_object_id: count.catalog_object_id,
    catalog_object_type: count.catalog_object_type ?? null,
    location_id: count.location_id ?? null,
    state: count.state ?? "UNKNOWN",
    quantity,
    calculated_at: calculatedAt,
    raw_payload: JSON.stringify(count),
  };
}

async function upsertInventoryRows(rows: InventoryRow[]) {
  if (rows.length === 0) {
    console.log("No inventory rows to upsert.");
    return;
  }

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    const text = `
      INSERT INTO pos.pos_inventory (
        tenant_id,
        provider,
        provider_account_id,
        catalog_object_id,
        catalog_object_type,
        location_id,
        state,
        quantity,
        calculated_at,
        raw_payload
      )
      VALUES (
        $1, $2, $3,
        $4, $5, $6,
        $7, $8, $9,
        $10
      )
      ON CONFLICT (tenant_id, provider, provider_account_id, catalog_object_id, location_id, state)
      DO UPDATE SET
        catalog_object_type = EXCLUDED.catalog_object_type,
        quantity            = EXCLUDED.quantity,
        calculated_at       = EXCLUDED.calculated_at,
        raw_payload         = EXCLUDED.raw_payload,
        updated_at          = CURRENT_TIMESTAMP;
    `;

    for (const row of rows) {
      const values = [
        TENANT_ID,
        POS_PROVIDER,
        POS_PROVIDER_ACCOUNT_ID,
        row.catalog_object_id,
        row.catalog_object_type,
        row.location_id,
        row.state,
        row.quantity,
        row.calculated_at,
        row.raw_payload,
      ];
      await client.query(text, values);
    }

    await client.query("COMMIT");
    console.log(`Upserted ${rows.length} inventory rows.`);
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}

async function main() {
  console.log("Fetching inventory counts from Square…");
  const counts: SquareInventoryCount[] = await fetchInventoryCounts();
  console.log(`Fetched ${counts.length} inventory counts from Square.`);

  const rows: InventoryRow[] = [];

  for (const count of counts) {
    const row = mapInventoryCountToRow(count);
    if (row) {
      rows.push(row);
    }
  }

  console.log(`Prepared ${rows.length} inventory rows to upsert…`);
  await upsertInventoryRows(rows);
}

main()
  .then(() => {
    console.log("Inventory ETL done.");
    return closePool();
  })
  .catch((err) => {
    console.error("Inventory ETL failed:", err);
    return closePool().finally(() => {
      process.exit(1);
    });
  });