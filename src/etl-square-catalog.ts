// src/etl-square-catalog.ts
import dotenv from "dotenv";
import { pool, closePool } from "./db.js";
import {
  fetchCatalogObjects,
  SquareCatalogObject,
} from "./square.js";

dotenv.config();

const TENANT_ID = process.env.TENANT_ID;
const POS_PROVIDER = process.env.POS_PROVIDER || "square";
const POS_PROVIDER_ACCOUNT_ID =
  process.env.POS_PROVIDER_ACCOUNT_ID || "default-square";

if (!TENANT_ID) {
  throw new Error("TENANT_ID is not set");
}

interface CatalogRow {
  catalog_object_id: string;
  object_type: string;
  item_name: string | null;
  variation_name: string | null;
  sku: string | null;
  is_deleted: boolean;
  raw_payload: string;
}

function mapVariationToRow(
  variation: SquareCatalogObject,
  parentName: string | null
): CatalogRow | null {
  if (!variation.id) {
    console.warn("Variation catalog object without id – skipping");
    return null;
  }

  const isDeleted = variation.is_deleted === true;

  const variationName = variation.item_variation_data?.name ?? null;
  const sku = variation.item_variation_data?.sku ?? null;

  // True item name comes from parent ITEM; fallback to variationName if missing
  const itemName = parentName ?? variationName ?? null;

  return {
    catalog_object_id: variation.id,
    object_type: variation.type,
    item_name: itemName,
    variation_name: variationName,
    sku,
    is_deleted: isDeleted,
    raw_payload: JSON.stringify(variation),
  };
}

async function upsertCatalogRows(rows: CatalogRow[]) {
  if (rows.length === 0) {
    console.log("No catalog rows to upsert.");
    return;
  }

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    const text = `
      INSERT INTO pos.pos_catalog (
        tenant_id,
        provider,
        provider_account_id,
        catalog_object_id,
        object_type,
        item_name,
        variation_name,
        sku,
        is_deleted,
        raw_payload
      )
      VALUES (
        $1, $2, $3,
        $4, $5, $6,
        $7, $8, $9,
        $10
      )
      ON CONFLICT (tenant_id, provider, provider_account_id, catalog_object_id)
      DO UPDATE SET
        object_type    = EXCLUDED.object_type,
        item_name      = EXCLUDED.item_name,
        variation_name = EXCLUDED.variation_name,
        sku            = EXCLUDED.sku,
        is_deleted     = EXCLUDED.is_deleted,
        raw_payload    = EXCLUDED.raw_payload;
    `;

    for (const row of rows) {
      const values = [
        TENANT_ID,
        POS_PROVIDER,
        POS_PROVIDER_ACCOUNT_ID,
        row.catalog_object_id,
        row.object_type,
        row.item_name,
        row.variation_name,
        row.sku,
        row.is_deleted,
        row.raw_payload,
      ];
      await client.query(text, values);
    }

    await client.query("COMMIT");
    console.log(`Upserted ${rows.length} catalog rows.`);
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}

async function main() {
  console.log("Fetching catalog ITEM and ITEM_VARIATION objects from Square…");
  // Fetch both ITEM and ITEM_VARIATION
  const objects: SquareCatalogObject[] = await fetchCatalogObjects("ITEM,ITEM_VARIATION");
  console.log(`Fetched ${objects.length} catalog objects from Square.`);

  const itemNameById = new Map<string, string>();

  // 1) Build map of ITEM.id -> ITEM.item_data.name
  for (const obj of objects) {
    if (obj.type === "ITEM") {
      const id = obj.id;
      const name = obj.item_data?.name;
      if (id && name) {
        itemNameById.set(id, name);
      }
    }
  }

  const rows: CatalogRow[] = [];

  // 2) Process ITEM_VARIATIONs, using parent item name when possible
  for (const obj of objects) {
    if (obj.type === "ITEM_VARIATION") {
      const parentItemId = obj.item_variation_data?.item_id ?? null;
      const parentName = parentItemId ? itemNameById.get(parentItemId) ?? null : null;

      const row = mapVariationToRow(obj, parentName);
      if (row) {
        rows.push(row);
      }
    }
  }

  console.log(`Prepared ${rows.length} variation rows to upsert…`);
  await upsertCatalogRows(rows);
}

main()
  .then(() => {
    console.log("Catalog ETL done.");
    return closePool();
  })
  .catch((err) => {
    console.error("Catalog ETL failed:", err);
    return closePool().finally(() => {
      process.exit(1);
    });
  });

