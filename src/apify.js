const APIFY_BASE_URL = "https://api.apify.com/v2";

function getApifyToken() {
  const token = String(process.env.APIFY_TOKEN || "").trim();
  if (!token) {
    throw new Error("Missing APIFY_TOKEN environment variable");
  }
  return token;
}

function asDatasetId(value) {
  if (value === undefined || value === null) {
    return null;
  }
  const out = String(value).trim();
  return out.length ? out : null;
}

async function fetchDatasetPage(datasetId, token, limit, offset) {
  const params = new URLSearchParams({
    token,
    clean: "true",
    format: "json",
    limit: String(limit),
    offset: String(offset),
  });

  const url = `${APIFY_BASE_URL}/datasets/${encodeURIComponent(datasetId)}/items?${params.toString()}`;
  const response = await fetch(url, {
    method: "GET",
    headers: {
      Accept: "application/json",
    },
  });

  if (!response.ok) {
    const raw = await response.text();
    throw new Error(`Apify API error ${response.status}: ${raw.slice(0, 300)}`);
  }

  const data = await response.json();
  if (!Array.isArray(data)) {
    throw new Error("Apify dataset response is not an array");
  }

  return data;
}

async function fetchDatasetItems(datasetId) {
  const parsedDatasetId = asDatasetId(datasetId);
  if (!parsedDatasetId) {
    throw new Error("datasetId is required");
  }

  const token = getApifyToken();
  const limit = 1000;
  let offset = 0;
  const out = [];

  while (true) {
    const page = await fetchDatasetPage(parsedDatasetId, token, limit, offset);
    if (page.length === 0) {
      break;
    }

    out.push(...page);
    offset += page.length;

    if (page.length < limit) {
      break;
    }
  }

  return out;
}

module.exports = {
  fetchDatasetItems,
};
