function asNullableString(value) {
  if (value === undefined || value === null) {
    return null;
  }
  const out = String(value).trim();
  return out.length ? out : null;
}

function normalizeToken(value) {
  const clean = asNullableString(value);
  if (!clean) {
    return null;
  }

  return clean
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "") || null;
}

function uniqueStrings(values) {
  const out = [];
  const seen = new Set();
  for (const value of values || []) {
    const clean = asNullableString(value);
    if (!clean) {
      continue;
    }
    const key = clean.toLowerCase();
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    out.push(clean);
  }
  return out;
}

function excerpt(text, maxLen = 400) {
  const clean = asNullableString(text) || "";
  if (clean.length <= maxLen) {
    return clean;
  }
  return `${clean.slice(0, maxLen)}...`;
}

const MONTHS = {
  enero: 1,
  febrero: 2,
  marzo: 3,
  abril: 4,
  mayo: 5,
  junio: 6,
  julio: 7,
  agosto: 8,
  septiembre: 9,
  setiembre: 9,
  octubre: 10,
  noviembre: 11,
  diciembre: 12,
};

const CITY_BY_KEYWORD = {
  rosario: "Rosario",
  funes: "Funes",
  perez: "Perez",
  "villa gobernador galvez": "Villa Gobernador Galvez",
  "granadero baigorria": "Granadero Baigorria",
  roldan: "Roldan",
  "san lorenzo": "San Lorenzo",
};

const CATEGORY_RULES = [
  { category: "teatro", keywords: ["teatro", "obra", "escena", "dramaturgia", "stand up"] },
  { category: "museo", keywords: ["museo", "muestra", "exposicion", "exposición", "galeria"] },
  { category: "musica", keywords: ["concierto", "recital", "musica", "música", "show", "orquesta"] },
  { category: "cine", keywords: ["cine", "pelicula", "película", "proyeccion", "proyección"] },
  { category: "feria", keywords: ["feria", "mercado", "artesania", "artesanía", "emprendedores"] },
  { category: "taller", keywords: ["taller", "workshop", "clase", "seminario"] },
  { category: "charla", keywords: ["charla", "conversatorio", "presentacion", "presentación", "encuentro"] },
  { category: "familiar", keywords: ["familiar", "infantil", "niños", "ninos", "familia"] },
  { category: "aire_libre", keywords: ["aire libre", "plaza", "parque", "costanera"] },
];

const FREE_PATTERNS = [
  /\bgratis\b/i,
  /\bentrada\s+libre\b/i,
  /\blibre\s+y\s+gratuit/iu,
  /\bsin\s+cargo\b/i,
  /\bno\s+arancelado\b/i,
];

const HOUR_REGEXES = [
  /\b(?:a\s+las\s*)?(\d{1,2})[:.](\d{2})\s*(?:hs?|h)?\b/i,
  /\b(?:a\s+las\s*)?(\d{1,2})\s*hs\b/i,
];

const DATE_REGEXES = [
  /\b(\d{1,2})\/(\d{1,2})(?:\/(\d{2,4}))?\b/,
  /\b(\d{1,2})\s+de\s+([a-zA-ZáéíóúÁÉÍÓÚñÑ]+)(?:\s+de\s+(\d{4}))?\b/u,
  /\b(20\d{2})-(\d{1,2})-(\d{1,2})\b/,
];

function normalizeHashtagTag(tag) {
  const clean = asNullableString(tag);
  if (!clean) {
    return null;
  }
  return clean.replace(/^#/, "").trim();
}

function parseHour(text) {
  const haystack = text || "";
  for (const re of HOUR_REGEXES) {
    const m = haystack.match(re);
    if (!m) {
      continue;
    }

    if (m.length >= 3 && m[2] !== undefined) {
      const h = String(m[1]).padStart(2, "0");
      const mm = String(m[2]).padStart(2, "0");
      return `${h}:${mm}`;
    }

    const h = Number(m[1]);
    if (Number.isFinite(h)) {
      return `${String(h).padStart(2, "0")}:00`;
    }
  }

  return null;
}

function parseEventDate(text, fallbackTimestamp) {
  const haystack = text || "";
  const currentYear = new Date().getUTCFullYear();

  for (const re of DATE_REGEXES) {
    const m = haystack.match(re);
    if (!m) {
      continue;
    }

    if (re === DATE_REGEXES[0]) {
      const day = Number(m[1]);
      const month = Number(m[2]);
      let year = Number(m[3] || currentYear);
      if (year < 100) {
        year += 2000;
      }
      const iso = toIsoDate(year, month, day);
      if (iso) {
        return {
          eventDate: iso,
          fechaText: m[0],
        };
      }
    }

    if (re === DATE_REGEXES[1]) {
      const day = Number(m[1]);
      const monthName = normalizeToken(m[2]);
      const month = monthName ? MONTHS[monthName] : null;
      const year = Number(m[3] || currentYear);
      const iso = month ? toIsoDate(year, month, day) : null;
      if (iso) {
        return {
          eventDate: iso,
          fechaText: m[0],
        };
      }
    }

    if (re === DATE_REGEXES[2]) {
      const year = Number(m[1]);
      const month = Number(m[2]);
      const day = Number(m[3]);
      const iso = toIsoDate(year, month, day);
      if (iso) {
        return {
          eventDate: iso,
          fechaText: m[0],
        };
      }
    }
  }

  const fallbackDate = asNullableString(fallbackTimestamp);
  if (fallbackDate && /^\d{4}-\d{2}-\d{2}/.test(fallbackDate)) {
    return {
      eventDate: fallbackDate.slice(0, 10),
      fechaText: fallbackDate.slice(0, 10),
    };
  }

  return {
    eventDate: null,
    fechaText: null,
  };
}

function toIsoDate(year, month, day) {
  if (!Number.isFinite(year) || !Number.isFinite(month) || !Number.isFinite(day)) {
    return null;
  }

  const date = new Date(Date.UTC(year, month - 1, day));
  if (
    date.getUTCFullYear() !== year ||
    date.getUTCMonth() !== month - 1 ||
    date.getUTCDate() !== day
  ) {
    return null;
  }

  return date.toISOString().slice(0, 10);
}

function inferCity(post, text) {
  const fromLocation = asNullableString(post.locationName);
  if (fromLocation) {
    const normalized = normalizeToken(fromLocation);
    for (const [keyword, value] of Object.entries(CITY_BY_KEYWORD)) {
      if (normalized && normalized.includes(normalizeToken(keyword))) {
        return value;
      }
    }
  }

  const haystack = normalizeToken(text || "") || "";
  for (const [keyword, value] of Object.entries(CITY_BY_KEYWORD)) {
    if (haystack.includes(normalizeToken(keyword))) {
      return value;
    }
  }

  return "Rosario";
}

function inferLugar(post, text) {
  const fromLocation = asNullableString(post.locationName);
  if (fromLocation) {
    return fromLocation;
  }

  const lineCandidates = String(text || "")
    .split(/\n+/)
    .map((line) => line.trim())
    .filter(Boolean);

  for (const line of lineCandidates) {
    if (/\b(museo|teatro|centro cultural|anfiteatro|auditorio|biblioteca|galeria|galería|plaza|parque)\b/i.test(line)) {
      return line.slice(0, 120);
    }
  }

  return null;
}

function inferCategory(text, hashtags) {
  const combined = `${text || ""}\n${(hashtags || []).join(" ")}`;
  const normalized = normalizeToken(combined) || "";
  let winner = "cultural";
  let score = 0;

  for (const rule of CATEGORY_RULES) {
    let localScore = 0;
    for (const keyword of rule.keywords) {
      const key = normalizeToken(keyword);
      if (!key) {
        continue;
      }
      if (normalized.includes(key)) {
        localScore += key.includes("_") ? 2 : 1;
      }
    }

    if (localScore > score) {
      score = localScore;
      winner = rule.category;
    }
  }

  return winner;
}

function inferTags(text, hashtags, category, isFree) {
  const tags = [];
  tags.push(...(hashtags || []).map(normalizeHashtagTag).filter(Boolean));

  if (category) {
    tags.push(category);
  }

  const normalized = normalizeToken(text || "") || "";
  for (const rule of CATEGORY_RULES) {
    for (const keyword of rule.keywords) {
      const key = normalizeToken(keyword);
      if (key && normalized.includes(key)) {
        tags.push(rule.category);
      }
    }
  }

  if (isFree) {
    tags.push("gratis");
  }

  return uniqueStrings(tags);
}

function inferIsFree(text, hashtags) {
  const haystack = `${text || ""}\n${(hashtags || []).join(" ")}`;
  for (const re of FREE_PATTERNS) {
    if (re.test(haystack)) {
      return true;
    }
  }
  return false;
}

function inferArtist(text, mentions) {
  const mention = Array.isArray(mentions) && mentions.length > 0 ? asNullableString(mentions[0]) : null;
  if (mention) {
    return mention.replace(/^@/, "");
  }

  const match = String(text || "").match(/\b(?:con|presenta|presentan|invitad[oa]s?)\s+([A-ZÁÉÍÓÚÑ][^\n,.]{2,80})/u);
  if (!match) {
    return null;
  }
  return asNullableString(match[1]);
}

function eventSignalScore(chunk) {
  let score = 0;
  if (parseHour(chunk)) {
    score += 1;
  }
  if (DATE_REGEXES.some((re) => re.test(chunk))) {
    score += 2;
  }
  if (/\b(museo|teatro|centro cultural|feria|concierto|muestra|taller|cine)\b/i.test(chunk)) {
    score += 1;
  }
  return score;
}

function splitCandidateBlocks(caption, ocrText) {
  const captionBlocks = String(caption || "")
    .split(/\n{2,}|[•·\-]{2,}/)
    .map((x) => x.trim())
    .filter(Boolean);

  const ocrBlocks = String(ocrText || "")
    .split(/\n{2,}/)
    .map((x) => x.trim())
    .filter(Boolean)
    .slice(0, 12);

  const blocks = uniqueStrings([...captionBlocks, ...ocrBlocks]);
  return blocks.filter((block) => eventSignalScore(block) > 0);
}

function buildBasePayload(post, ocrText, caption) {
  return {
    source_post_id: asNullableString(post.postId || post.id || post.shortCode),
    source_owner_username: asNullableString(post.ownerUsername),
    source_url: asNullableString(post.url),
    ocr_text_excerpt: excerpt(ocrText, 600),
    caption_excerpt: excerpt(caption, 600),
  };
}

function buildEventFromChunk(post, chunk, context) {
  const parsedDate = parseEventDate(chunk, post.timestamp);
  const hora = parseHour(chunk) || parseHour(context.fullText);
  const ciudad = inferCity(post, `${chunk}\n${context.fullText}`);
  const lugar = inferLugar(post, `${chunk}\n${context.fullText}`);
  const categoria = inferCategory(`${chunk}\n${context.fullText}`, post.hashtags || []);
  const isFree = inferIsFree(`${chunk}\n${context.fullText}`, post.hashtags || []);
  const artista = inferArtist(`${chunk}\n${context.caption}`, post.mentions || []);
  const tags = inferTags(`${chunk}\n${context.fullText}`, post.hashtags || [], categoria, isFree);

  return {
    categoria,
    fecha_text: parsedDate.fechaText || asNullableString(post.timestamp),
    hora,
    ciudad,
    lugar,
    event_date: parsedDate.eventDate,
    payload: {
      ...context.payloadBase,
      evidence_excerpt: excerpt(chunk, 400),
    },
    category_norm: normalizeToken(categoria) || "sin_categoria",
    ciudad_norm: normalizeToken(ciudad) || "rosario",
    lugar_norm: normalizeToken(lugar),
    tipo_norm: normalizeToken(categoria) || "evento",
    artista_norm: normalizeToken(artista),
    is_free: isFree,
    tags,
  };
}

function dedupeEvents(events) {
  const map = new Map();

  for (const event of events) {
    const key = [
      normalizeToken(event.categoria) || "sin_categoria",
      normalizeToken(event.fecha_text) || "sin_fecha",
      normalizeToken(event.hora) || "sin_hora",
      normalizeToken(event.lugar) || "sin_lugar",
      normalizeToken(event.artista_norm) || "sin_artista",
    ].join("|");

    if (!map.has(key)) {
      map.set(key, event);
    }
  }

  return Array.from(map.values());
}

function extractEventsFromPost(post) {
  const caption = asNullableString(post.caption) || "";
  const ocrText = asNullableString(post.ocrText) || "";
  const fullText = [caption, ocrText, asNullableString(post.locationName) || "", asNullableString(post.ownerUsername) || ""]
    .filter(Boolean)
    .join("\n");

  const blocks = splitCandidateBlocks(caption, ocrText);
  const context = {
    caption,
    fullText,
    payloadBase: buildBasePayload(post, ocrText, caption),
  };

  const candidates = [];
  for (const block of blocks) {
    candidates.push(buildEventFromChunk(post, block, context));
  }

  if (candidates.length === 0) {
    const fallback = buildEventFromChunk(post, fullText, context);
    if (fallback.fecha_text || fallback.hora || fallback.lugar) {
      candidates.push(fallback);
    }
  }

  return dedupeEvents(candidates);
}

module.exports = {
  extractEventsFromPost,
};
