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
  category_id: string | null;  // ← ADD THIS LINE
  is_deleted: boolean;
  raw_payload: string;
}

function mapVariationToRow(
  variation: SquareCatalogObject,
  parentName: string | null,
  parentCategoryId: string | null  // ← ADD THIS PARAMETER
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
    category_id: parentCategoryId,  // ← ADD THIS LINE
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
        category_id,
        is_deleted,
        raw_payload
      )
      VALUES (
        $1, $2, $3,
        $4, $5, $6,
        $7, $8, $9,
        $10, $11
      )
      ON CONFLICT (tenant_id, provider, provider_account_id, catalog_object_id)
      DO UPDATE SET
        object_type    = EXCLUDED.object_type,
        item_name      = EXCLUDED.item_name,
        variation_name = EXCLUDED.variation_name,
        sku            = EXCLUDED.sku,
        category_id    = EXCLUDED.category_id,
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
        row.category_id,  // ← ADD THIS LINE
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
  const objects: SquareCatalogObject[] = await fetchCatalogObjects("ITEM,ITEM_VARIATION");
  console.log(`Fetched ${objects.length} catalog objects from Square.`);

  const itemNameById = new Map<string, string>();
  const itemCategoryById = new Map<string, string>();  // ← ADD THIS LINE

  // 1) Build maps of ITEM.id -> ITEM.item_data.name and ITEM.id -> category_id
  for (const obj of objects) {
    if (obj.type === "ITEM") {
      const id = obj.id;
      const name = obj.item_data?.name;
      const categoryId = obj.item_data?.category_id;  // ← ADD THIS LINE
      
      if (id && name) {
        itemNameById.set(id, name);
      }
      if (id && categoryId) {  // ← ADD THIS BLOCK
        itemCategoryById.set(id, categoryId);
      }
    }
  }

  const rows: CatalogRow[] = [];

  // 2) Process ITEM_VARIATIONs, using parent item name and category when possible
  for (const obj of objects) {
    if (obj.type === "ITEM_VARIATION") {
      const parentItemId = obj.item_variation_data?.item_id ?? null;
      const parentName = parentItemId ? itemNameById.get(parentItemId) ?? null : null;
      const parentCategoryId = parentItemId ? itemCategoryById.get(parentItemId) ?? null : null;  // ← ADD THIS LINE

      const row = mapVariationToRow(obj, parentName, parentCategoryId);  // ← UPDATE THIS LINE
      if (row) {
        rows.push(row);
      }
    }
  }

  // After line 155 (after processing ITEM_VARIATIONs), add this:
for (const obj of objects) {
  if (obj.type === "ITEM") {
    const row: CatalogRow = {
      catalog_object_id: obj.id,
      object_type: obj.type,
      item_name: obj.item_data?.name ?? null,
      variation_name: null,
      sku: null,
      category_id: obj.item_data?.category_id ?? null,
      is_deleted: obj.is_deleted ?? false,
      raw_payload: JSON.stringify(obj),
    };
    rows.push(row);
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

