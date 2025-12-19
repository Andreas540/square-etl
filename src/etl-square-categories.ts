// src/etl-square-categories.ts
import dotenv from "dotenv";
import { pool, closePool } from "./db.js";
import {
  fetchCategories,
  SquareCategoryObject,
} from "./square.js";

dotenv.config();

const TENANT_ID = process.env.TENANT_ID;
const POS_PROVIDER = process.env.POS_PROVIDER || "square";
const POS_PROVIDER_ACCOUNT_ID =
  process.env.POS_PROVIDER_ACCOUNT_ID || "default-square";

if (!TENANT_ID) {
  throw new Error("TENANT_ID is not set");
}

interface CategoryRow {
  category_id: string;
  category_name: string;
  parent_category_id: string | null;
  is_top_level: boolean;
  is_deleted: boolean;
  raw_payload: string;
}

function mapCategoryToRow(
  category: SquareCategoryObject
): CategoryRow | null {
  if (!category.id) {
    console.warn("Category object without id – skipping");
    return null;
  }

  const isDeleted = category.is_deleted === true;
  const categoryName = category.category_data?.name ?? "Unknown Category";
  const isTopLevel = category.category_data?.is_top_level ?? true;
  
  // Note: parent_category_id would come from category.category_data.parent_category
  // But the structure might be different - check your actual Square data
  const parentCategoryId = null; // TODO: Extract if Square provides this

  return {
    category_id: category.id,
    category_name: categoryName,
    parent_category_id: parentCategoryId,
    is_top_level: isTopLevel,
    is_deleted: isDeleted,
    raw_payload: JSON.stringify(category),
  };
}

async function upsertCategoryRows(rows: CategoryRow[]) {
  if (rows.length === 0) {
    console.log("No category rows to upsert.");
    return;
  }

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    const text = `
      INSERT INTO pos.pos_categories (
        tenant_id,
        provider,
        provider_account_id,
        category_id,
        category_name,
        parent_category_id,
        is_top_level,
        is_deleted,
        raw_payload
      )
      VALUES (
        $1, $2, $3,
        $4, $5, $6,
        $7, $8, $9
      )
      ON CONFLICT (tenant_id, provider, provider_account_id, category_id)
      DO UPDATE SET
        category_name       = EXCLUDED.category_name,
        parent_category_id  = EXCLUDED.parent_category_id,
        is_top_level        = EXCLUDED.is_top_level,
        is_deleted          = EXCLUDED.is_deleted,
        raw_payload         = EXCLUDED.raw_payload,
        updated_at          = CURRENT_TIMESTAMP;
    `;

    for (const row of rows) {
      const values = [
        TENANT_ID,
        POS_PROVIDER,
        POS_PROVIDER_ACCOUNT_ID,
        row.category_id,
        row.category_name,
        row.parent_category_id,
        row.is_top_level,
        row.is_deleted,
        row.raw_payload,
      ];
      await client.query(text, values);
    }

    await client.query("COMMIT");
    console.log(`Upserted ${rows.length} category rows.`);
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}

async function main() {
  console.log("Fetching categories from Square…");
  const categories: SquareCategoryObject[] = await fetchCategories();
  console.log(`Fetched ${categories.length} categories from Square.`);

  const rows: CategoryRow[] = [];

  for (const category of categories) {
    const row = mapCategoryToRow(category);
    if (row) {
      rows.push(row);
    }
  }

  console.log(`Prepared ${rows.length} category rows to upsert…`);
  await upsertCategoryRows(rows);
}

main()
  .then(() => {
    console.log("Categories ETL done.");
    return closePool();
  })
  .catch((err) => {
    console.error("Categories ETL failed:", err);
    return closePool().finally(() => {
      process.exit(1);
    });
  });