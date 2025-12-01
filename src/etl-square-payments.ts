// src/etl-square-payments.ts
import dotenv from "dotenv";
import { pool, closePool } from "./db.js";
import { fetchPaymentsPaged, SquarePayment } from "./square.js";

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

function mapPaymentToRow(p: SquarePayment) {
  // Vi använder total_money om det finns, annars amount_money
  const money = p.total_money ?? p.amount_money;
  if (!money) {
    throw new Error(`Payment ${p.id} has no money fields`);
  }

  return {
    payment_id: p.id,
    order_id: p.order_id ?? null,
    location_id: p.location_id ?? null,
    created_at: p.created_at,
    updated_at: p.updated_at ?? null,
    amount: money.amount,
    currency: money.currency,
    status: p.status ?? null,
    customer_id: p.customer_id ?? null,
    reference_id: p.reference_id ?? null,
    raw_payload: JSON.stringify(p),
  };
}

async function upsertPayments(payments: SquarePayment[]) {
  if (payments.length === 0) {
    console.log("No payments to upsert.");
    return;
  }

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    const text = `
      INSERT INTO pos.pos_payments (
        tenant_id,
        provider,
        provider_account_id,
        payment_id,
        order_id,
        location_id,
        created_at,
        updated_at,
        amount,
        currency,
        status,
        customer_id,
        reference_id,
        raw_payload
      )
      VALUES (
        $1, $2, $3,
        $4, $5, $6,
        $7, $8, $9, $10,
        $11, $12, $13, $14
      )
      ON CONFLICT (tenant_id, provider, payment_id)
      DO UPDATE SET
        provider_account_id = EXCLUDED.provider_account_id,
        order_id           = EXCLUDED.order_id,
        location_id        = EXCLUDED.location_id,
        created_at         = EXCLUDED.created_at,
        updated_at         = EXCLUDED.updated_at,
        amount             = EXCLUDED.amount,
        currency           = EXCLUDED.currency,
        status             = EXCLUDED.status,
        customer_id        = EXCLUDED.customer_id,
        reference_id       = EXCLUDED.reference_id,
        raw_payload        = EXCLUDED.raw_payload;
    `;

    for (const p of payments) {
      const row = mapPaymentToRow(p);

      const values = [
        TENANT_ID,
        POS_PROVIDER,
        POS_PROVIDER_ACCOUNT_ID,
        row.payment_id,
        row.order_id,
        row.location_id,
        row.created_at,
        row.updated_at,
        row.amount,
        row.currency,
        row.status,
        row.customer_id,
        row.reference_id,
        row.raw_payload,
      ];

      await client.query(text, values);
    }

    await client.query("COMMIT");
    console.log(`Upserted ${payments.length} payments.`);
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}

async function main() {
  const { begin, end } = getTimeWindow(SYNC_LOOKBACK_HOURS);
  console.log(`Fetching payments from Square between ${begin} and ${end}…`);

  const payments = await fetchPaymentsPaged(begin, end);
  console.log(`Fetched ${payments.length} payments from Square.`);

  await upsertPayments(payments);
}

main()
  .then(() => {
    console.log("Done.");
    return closePool();
  })
  .catch((err) => {
    console.error("ETL failed:", err);
    return closePool().finally(() => {
      process.exit(1);
    });
  });
