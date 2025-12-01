// src/db.ts
import { Pool } from "pg";
import dotenv from "dotenv";

dotenv.config();

const connectionString = process.env.NEON_DATABASE_URL;
if (!connectionString) {
  throw new Error("NEON_DATABASE_URL is not set");
}

export const pool = new Pool({
  connectionString,
  ssl: {
    rejectUnauthorized: false, // Neon brukar kräva SSL
  },
});

export async function closePool() {
  await pool.end();
}
