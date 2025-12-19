// src/square.ts
import dotenv from "dotenv";

dotenv.config();

const SQUARE_BASE_URL = "https://connect.squareup.com";

const SQUARE_ACCESS_TOKEN = process.env.SQUARE_ACCESS_TOKEN;
const SQUARE_API_VERSION = process.env.SQUARE_API_VERSION || "2025-01-15";

if (!SQUARE_ACCESS_TOKEN) {
  throw new Error("SQUARE_ACCESS_TOKEN is not set");
}

export interface SquareMoney {
  amount: number;
  currency: string;
}

/** ----- PAYMENTS ----- **/

export interface SquarePayment {
  id: string;
  created_at: string;
  updated_at?: string;
  location_id?: string;
  order_id?: string;
  status?: string;
  customer_id?: string;
  reference_id?: string;
  amount_money?: SquareMoney;
  total_money?: SquareMoney;
}

interface PaymentsResponse {
  payments?: SquarePayment[];
  cursor?: string;
}

/**
 * Fetch all payments in a time window, handling pagination and rate limiting.
 */
export async function fetchPaymentsPaged(
  beginTimeISO: string,
  endTimeISO?: string
): Promise<SquarePayment[]> {
  const all: SquarePayment[] = [];
  let cursor: string | undefined = undefined;

  do {
    const url = new URL(`${SQUARE_BASE_URL}/v2/payments`);

    url.searchParams.set("begin_time", beginTimeISO);
    if (endTimeISO) url.searchParams.set("end_time", endTimeISO);
    url.searchParams.set("sort_order", "ASC");
    if (cursor) {
      url.searchParams.set("cursor", cursor);
    }

    const res = await fetch(url.toString(), {
      method: "GET",
      headers: {
        Authorization: `Bearer ${SQUARE_ACCESS_TOKEN}`,
        "Square-Version": SQUARE_API_VERSION,
        "Content-Type": "application/json",
      },
    });

    if (res.status === 429) {
      console.warn("Rate limited by Square (payments), waiting 10s…");
      await new Promise((r) => setTimeout(r, 10_000));
      continue;
    }

    if (!res.ok) {
      const body = await res.text();
      throw new Error(
        `Square payments request failed: ${res.status} ${res.statusText} – ${body}`
      );
    }

    const data = (await res.json()) as PaymentsResponse;
    if (data.payments && data.payments.length > 0) {
      all.push(...data.payments);
    }

    cursor = data.cursor;
  } while (cursor);

  return all;
}

/** ----- ORDERS ----- **/

export interface SquareLineItem {
  uid?: string;
  name?: string;
  catalog_object_id?: string;
  quantity?: string; // we will parse float
  base_price_money?: SquareMoney;
  total_money?: SquareMoney;
}

export interface SquareOrder {
  id: string;
  location_id?: string;
  line_items?: SquareLineItem[];
}

interface OrderResponse {
  order?: SquareOrder;
}

/**
 * Fetch a single order by ID (to get line items).
 */
export async function fetchOrder(orderId: string): Promise<SquareOrder | null> {
  const url = new URL(`${SQUARE_BASE_URL}/v2/orders/${orderId}`);

  const res = await fetch(url.toString(), {
    method: "GET",
    headers: {
      Authorization: `Bearer ${SQUARE_ACCESS_TOKEN}`,
      "Square-Version": SQUARE_API_VERSION,
      "Content-Type": "application/json",
    },
  });

  if (res.status === 429) {
    console.warn(
      `Rate limited by Square (order ${orderId}), waiting 10s then retry…`
    );
    await new Promise((r) => setTimeout(r, 10_000));
    return fetchOrder(orderId); // simple retry once after backoff
  }

  if (res.status === 404) {
    console.warn(`Order ${orderId} not found in Square.`);
    return null;
  }

  if (!res.ok) {
    const body = await res.text();
    throw new Error(
      `Square order request failed for ${orderId}: ${res.status} ${res.statusText} – ${body}`
    );
  }

  const data = (await res.json()) as OrderResponse;
  if (!data.order) {
    console.warn(`Order ${orderId} returned no 'order' object.`);
    return null;
  }

  return data.order;
}
/** ----- CATALOG ----- **/

export interface SquareCatalogObject {
  id: string;
  type: string;
  is_deleted?: boolean;
  item_data?: {
    name?: string;
    categories?: Array<{   // ← CHANGE THIS
      id: string;
      ordinal?: number;
    }>;
  };
  item_variation_data?: {
    name?: string;
    sku?: string;
    item_id?: string;
  };
}

interface CatalogResponse {
  objects?: SquareCatalogObject[];
  cursor?: string;
}

/**
 * Fetch catalog objects from Square.
 * By default we fetch only ITEM_VARIATION objects (things that can be sold).
 */
export async function fetchCatalogObjects(
  types: string = "ITEM_VARIATION"
): Promise<SquareCatalogObject[]> {
  const all: SquareCatalogObject[] = [];
  let cursor: string | undefined = undefined;

  do {
    const url = new URL(`${SQUARE_BASE_URL}/v2/catalog/list`);

    url.searchParams.set("types", types);
    if (cursor) {
      url.searchParams.set("cursor", cursor);
    }

    const res = await fetch(url.toString(), {
      method: "GET",
      headers: {
        Authorization: `Bearer ${SQUARE_ACCESS_TOKEN}`,
        "Square-Version": SQUARE_API_VERSION,
        "Content-Type": "application/json",
      },
    });

    if (res.status === 429) {
      console.warn("Rate limited by Square (catalog), waiting 10s…");
      await new Promise((r) => setTimeout(r, 10_000));
      continue;
    }

    if (!res.ok) {
      const body = await res.text();
      throw new Error(
        `Square catalog request failed: ${res.status} ${res.statusText} – ${body}`
      );
    }

    const data = (await res.json()) as CatalogResponse;
    if (data.objects && data.objects.length > 0) {
      all.push(...data.objects);
    }

    cursor = data.cursor;
  } while (cursor);

  return all;
}

// Add to src/square.ts (at the end, before the final export or at the bottom)

/** ----- INVENTORY ----- **/

export interface SquareInventoryCount {
  catalog_object_id?: string;
  catalog_object_type?: string;
  state?: string; // e.g., "IN_STOCK", "SOLD", "WASTE"
  location_id?: string;
  quantity?: string;
  calculated_at?: string;
}

interface BatchRetrieveInventoryCountsResponse {
  counts?: SquareInventoryCount[];
  cursor?: string;
}

/**
 * Fetch inventory counts for all catalog items across all locations.
 * Uses BatchRetrieveInventoryCounts endpoint with pagination.
 */
export async function fetchInventoryCounts(): Promise<SquareInventoryCount[]> {
  const all: SquareInventoryCount[] = [];
  let cursor: string | undefined = undefined;

  do {
    const url = new URL(`${SQUARE_BASE_URL}/v2/inventory/counts/batch-retrieve`);

    const body: any = {
      // Leave catalog_object_ids empty to fetch all items
      // Leave location_ids empty to fetch all locations
    };

    if (cursor) {
      body.cursor = cursor;
    }

    const res = await fetch(url.toString(), {
      method: "POST",
      headers: {
        Authorization: `Bearer ${SQUARE_ACCESS_TOKEN}`,
        "Square-Version": SQUARE_API_VERSION,
        "Content-Type": "application/json",
      },
      body: JSON.stringify(body),
    });

    if (res.status === 429) {
      console.warn("Rate limited by Square (inventory), waiting 10s…");
      await new Promise((r) => setTimeout(r, 10_000));
      continue;
    }

    if (!res.ok) {
      const body = await res.text();
      throw new Error(
        `Square inventory request failed: ${res.status} ${res.statusText} – ${body}`
      );
    }

    const data = (await res.json()) as BatchRetrieveInventoryCountsResponse;
    if (data.counts && data.counts.length > 0) {
      all.push(...data.counts);
    }

    cursor = data.cursor;
  } while (cursor);

  return all;
}

/** ----- CATEGORIES ----- **/

export interface SquareCategoryObject {
  id: string;
  type: string;
  is_deleted?: boolean;
  category_data?: {
    name?: string;
    parent_category?: {
      ordinal?: number;
    };
    is_top_level?: boolean;
  };
}

interface CategoryResponse {
  objects?: SquareCategoryObject[];
  cursor?: string;
}

/**
 * Fetch category objects from Square Catalog.
 */
export async function fetchCategories(): Promise<SquareCategoryObject[]> {
  const all: SquareCategoryObject[] = [];
  let cursor: string | undefined = undefined;

  do {
    const url = new URL(`${SQUARE_BASE_URL}/v2/catalog/list`);

    url.searchParams.set("types", "CATEGORY");
    if (cursor) {
      url.searchParams.set("cursor", cursor);
    }

    const res = await fetch(url.toString(), {
      method: "GET",
      headers: {
        Authorization: `Bearer ${SQUARE_ACCESS_TOKEN}`,
        "Square-Version": SQUARE_API_VERSION,
        "Content-Type": "application/json",
      },
    });

    if (res.status === 429) {
      console.warn("Rate limited by Square (categories), waiting 10s…");
      await new Promise((r) => setTimeout(r, 10_000));
      continue;
    }

    if (!res.ok) {
      const body = await res.text();
      throw new Error(
        `Square categories request failed: ${res.status} ${res.statusText} – ${body}`
      );
    }

    const data = (await res.json()) as CategoryResponse;
    if (data.objects && data.objects.length > 0) {
      all.push(...data.objects);
    }

    cursor = data.cursor;
  } while (cursor);

  return all;
}

/** ----- LOCATIONS ----- **/

export interface SquareLocation {
  id?: string;
  name?: string;
  address?: {
    address_line_1?: string;
    locality?: string;
    administrative_district_level_1?: string;
    postal_code?: string;
  };
  timezone?: string;
  status?: string;
}

interface LocationsResponse {
  locations?: SquareLocation[];
}

/**
 * Fetch all locations from Square.
 */
export async function fetchLocations(): Promise<SquareLocation[]> {
  const url = new URL(`${SQUARE_BASE_URL}/v2/locations`);

  const res = await fetch(url.toString(), {
    method: "GET",
    headers: {
      Authorization: `Bearer ${SQUARE_ACCESS_TOKEN}`,
      "Square-Version": SQUARE_API_VERSION,
      "Content-Type": "application/json",
    },
  });

  if (res.status === 429) {
    console.warn("Rate limited by Square (locations), waiting 10s…");
    await new Promise((r) => setTimeout(r, 10_000));
    return fetchLocations(); // Retry
  }

  if (!res.ok) {
    const body = await res.text();
    throw new Error(
      `Square locations request failed: ${res.status} ${res.statusText} – ${body}`
    );
  }

  const data = (await res.json()) as LocationsResponse;
  return data.locations || [];
}