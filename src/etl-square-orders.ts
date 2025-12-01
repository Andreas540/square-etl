// src/etl-square-orders.ts
import dotenv from "dotenv";
import { pool, closePool } from "./db.js";
import {
  fetchPaymentsPaged,
  fetchOrder,
  SquarePayment,
  SquareOrder,
  SquareLineItem,
} from "./square.js";

dotenv.config();

const TENANT_ID = process.env.TENANT_ID;
const POS_PROVIDER = process.env.POS_PROVIDER || "square";
const POS_PROVIDER_ACCOUNT_ID =
  process.env.POS_PROVIDER_ACCOUNT_ID || "default-square";
const SYNC_LOOKBACK_HOURS = Number(process.env.SYNC_LOOKBACK_HOURS || "24");

if (!TENANT_ID) {
  throw new Error("TENANT_ID is not set");
}

function getTimeWindow(hours: number): { begin: string; end: string } {
  const end = new Date();
  const begin = new Date(end.getTime() - hours * 60 * 60 * 1000);
  return {
    begin: begin.toISOString(),
    end: end.toISOString(),
  };
}

interface OrderItemRow {
  order_id: string;
  payment_id: string | null;
  line_item_uid: string;
  catalog_object_id: string | null;
  item_name: string | null;
  sku: string | null;
  quantity: number;
  base_price_amount: number | null;
  total_money_amount: number | null;
  currency: string | null;
  location_id: string | null;
  raw_payload: string;
}

function mapLineItemToRow(
  order: SquareOrder,
  paymentId: string | null,
  lineItem: SquareLineItem
): OrderItemRow | null {
  const uid = lineItem.uid;
  if (!uid) {
    console.warn(
      `Order ${order.id} has line item without uid – skipping that line.`
    );
    return null;
  }

  const quantityStr = lineItem.quantity ?? "0";
  const quantity = parseFloat(quantityStr);
  if (!Number.isFinite(quantity) || quantity <= 0) {
    console.warn(
      `Order ${order.id}, line ${uid} has invalid quantity '${quantityStr}' – skipping.`
    );
    return null;
  }

  const baseAmount = lineItem.base_price_money?.amount ?? null;
  const totalAmount = lineItem.total_money?.amount ?? null;
  const currency = lineItem.base_price_money?.currency
    ?? lineItem.total_money?.currency
    ?? null;

  return {
    order_id: order.id,
    payment_id: paymentId,
    line_item_uid: uid,
    catalog_object_id: lineItem.catalog_object_id ?? null,
    item_name: lineItem.name ?? null,
    sku: null, // we can fill this later from Catalog API
    quantity,
    base_price_amount: baseAmount,
    total_money_amount: totalAmount,
    currency,
    location_id: order.location_id ?? null,
    raw_payload: JSON.stringify(lineItem),
  };
}

async function upsertOrderItems(rows: OrderItemRow[]) {
  if (rows.length === 0) {
    console.log("No order items to upsert.");
    return;
  }

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    const text = `
      INSERT INTO pos.pos_order_items (
        tenant_id,
        provider,
        provider_account_id,
        order_id,
        payment_id,
        line_item_uid,
        catalog_object_id,
        item_name,
        sku,
        quantity,
        base_price_amount,
        total_money_amount,
        currency,
        location_id,
        raw_payload
      )
      VALUES (
        $1, $2, $3,
        $4, $5, $6,
        $7, $8, $9,
        $10, $11, $12,
        $13, $14, $15
      )
      ON CONFLICT (tenant_id, provider, order_id, line_item_uid)
      DO UPDATE SET
        provider_account_id = EXCLUDED.provider_account_id,
        payment_id          = EXCLUDED.payment_id,
        catalog_object_id   = EXCLUDED.catalog_object_id,
        item_name           = EXCLUDED.item_name,
        sku                 = EXCLUDED.sku,
        quantity            = EXCLUDED.quantity,
        base_price_amount   = EXCLUDED.base_price_amount,
        total_money_amount  = EXCLUDED.total_money_amount,
        currency            = EXCLUDED.currency,
        location_id         = EXCLUDED.location_id,
        raw_payload         = EXCLUDED.raw_payload;
    `;

    for (const row of rows) {
      const values = [
        TENANT_ID,
        POS_PROVIDER,
        POS_PROVIDER_ACCOUNT_ID,
        row.order_id,
        row.payment_id,
        row.line_item_uid,
        row.catalog_object_id,
        row.item_name,
        row.sku,
        row.quantity,
        row.base_price_amount,
        row.total_money_amount,
        row.currency,
        row.location_id,
        row.raw_payload,
      ];

      await client.query(text, values);
    }

    await client.query("COMMIT");
    console.log(`Upserted ${rows.length} order items.`);
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}

async function main() {
  const { begin, end } = getTimeWindow(SYNC_LOOKBACK_HOURS);
  console.log(`Fetching payments (for orders) between ${begin} and ${end}…`);

  const payments: SquarePayment[] = await fetchPaymentsPaged(begin, end);
  console.log(`Fetched ${payments.length} payments from Square.`);

  // Map order_id -> payment_id (first one wins if multiple)
  const orderToPayment = new Map<string, string>();

  for (const p of payments) {
    if (p.order_id) {
      if (!orderToPayment.has(p.order_id)) {
        orderToPayment.set(p.order_id, p.id);
      }
    }
  }

  const uniqueOrderIds = Array.from(orderToPayment.keys());
  console.log(`Found ${uniqueOrderIds.length} unique orders with payments.`);

  const allRows: OrderItemRow[] = [];

  for (const orderId of uniqueOrderIds) {
    const paymentId = orderToPayment.get(orderId) ?? null;
    const order = await fetchOrder(orderId);
    if (!order) continue;

    if (!order.line_items || order.line_items.length === 0) {
      console.log(`Order ${orderId} has no line_items – skipping.`);
      continue;
    }

    for (const li of order.line_items) {
      const row = mapLineItemToRow(order, paymentId, li);
      if (row) {
        allRows.push(row);
      }
    }
  }

  console.log(`Prepared ${allRows.length} order item rows to upsert…`);
  await upsertOrderItems(allRows);
}

main()
  .then(() => {
    console.log("Order ETL done.");
    return closePool();
  })
  .catch((err) => {
    console.error("Order ETL failed:", err);
    return closePool().finally(() => {
      process.exit(1);
    });
  });
