const { assertRecommendationResult, assertBotResponse } = require("./contracts");

function asNonEmptyString(value) {
  if (value === undefined || value === null) {
    return null;
  }
  const out = String(value).trim();
  return out.length > 0 ? out : null;
}

function asNumberOrNull(value) {
  if (typeof value !== "number" || Number.isNaN(value) || !Number.isFinite(value)) {
    return null;
  }
  return value;
}

function splitTextIntoChunks(text, maxChunkLength) {
  const cleanText = String(text || "").trim();
  if (!cleanText) {
    return [""];
  }

  if (cleanText.length <= maxChunkLength) {
    return [cleanText];
  }

  const chunks = [];
  let remaining = cleanText;

  while (remaining.length > maxChunkLength) {
    let splitAt = remaining.lastIndexOf("\n", maxChunkLength);
    if (splitAt < Math.floor(maxChunkLength * 0.5)) {
      splitAt = remaining.lastIndexOf(" ", maxChunkLength);
    }
    if (splitAt < Math.floor(maxChunkLength * 0.5)) {
      splitAt = maxChunkLength;
    }

    const chunk = remaining.slice(0, splitAt).trim();
    if (chunk) {
      chunks.push(chunk);
    }
    remaining = remaining.slice(splitAt).trim();
  }

  if (remaining) {
    chunks.push(remaining);
  }

  return chunks.length > 0 ? chunks : [cleanText];
}

function formatEventLine(index, event) {
  const title = asNonEmptyString(event.title) || "Evento sin titulo";
  const dateValue = asNonEmptyString(event.eventDate) || asNonEmptyString(event.dateText);
  const timeValue = asNonEmptyString(event.timeText);
  const venueValue = asNonEmptyString(event.venue);
  const cityValue = asNonEmptyString(event.city);

  const details = [];
  if (dateValue) {
    details.push(`fecha: ${dateValue}`);
  }
  if (timeValue) {
    details.push(`hora: ${timeValue}`);
  }
  if (venueValue) {
    details.push(`lugar: ${venueValue}`);
  }
  if (cityValue && cityValue.toLowerCase() !== "rosario") {
    details.push(`ciudad: ${cityValue}`);
  }

  if (details.length === 0) {
    return `${index}. ${title}`;
  }

  return `${index}. ${title} (${details.join(" | ")})`;
}

function renderOkStatus(result) {
  const lines = [];

  lines.push("Te comparto opciones culturales:");
  lines.push("");

  result.shortlist.forEach((item, index) => {
    lines.push(formatEventLine(index + 1, item.event || {}));
  });

  if (result.fallbackUsed) {
    lines.push("");
    lines.push("Nota: no hubo coincidencia exacta para la fecha pedida y use un fallback controlado.");
  }

  if (asNonEmptyString(result.note)) {
    lines.push("");
    lines.push(result.note.trim());
  }

  return lines.join("\n").trim();
}

function renderEmptyStatus(result) {
  const lines = [];

  lines.push("No encontre eventos que cumplan esos filtros por ahora.");

  if (result.fallbackUsed) {
    lines.push("Tambien probe un fallback controlado y no aparecieron opciones relevantes.");
  }

  if (result.needsAction === "relax_filters") {
    lines.push("Si queres, puedo buscar con filtros mas flexibles (dia, categoria o horario).");
  }

  if (asNonEmptyString(result.note)) {
    lines.push(result.note.trim());
  }

  return lines.join("\n").trim();
}

function renderNeedUserInputStatus(result) {
  if (result.needsAction === "share_location") {
    return "Para recomendarte opciones cerca, compartime tu ubicacion.";
  }

  if (result.needsAction === "relax_filters") {
    return "Necesito que flexibilices algunos filtros para poder encontrarte opciones.";
  }

  if (asNonEmptyString(result.note)) {
    return result.note.trim();
  }

  return "Necesito un dato mas para continuar con la recomendacion.";
}

function renderText(result) {
  if (result.status === "ok") {
    return renderOkStatus(result);
  }

  if (result.status === "empty") {
    return renderEmptyStatus(result);
  }

  if (result.status === "need_user_input") {
    return renderNeedUserInputStatus(result);
  }

  if (asNonEmptyString(result.note)) {
    return result.note.trim();
  }

  return "No pude procesar la recomendacion en este momento.";
}

function createResponseRenderer({
  maxChunkLength = 1200,
  defaultParseMode = null,
} = {}) {
  const cleanChunkLength = Number.isInteger(maxChunkLength) && maxChunkLength > 0 ? maxChunkLength : 1200;

  async function render(recommendationResult, context = {}) {
    const result = assertRecommendationResult(recommendationResult);

    const inferredChatId =
      asNumberOrNull(context.chatId) ||
      asNumberOrNull(result.chatId) ||
      asNumberOrNull(result.debug && result.debug.chatId) ||
      0;

    const text = renderText(result);
    const chunks = splitTextIntoChunks(text, cleanChunkLength);

    const response = {
      chatId: inferredChatId,
      text,
      parseMode: defaultParseMode,
      chunks,
      metadata: {
        recommendationStatus: result.status,
        usedFallback: Boolean(result.fallbackUsed),
        needsAction: result.needsAction,
        candidatesCount: result.candidatesCount,
      },
    };

    return assertBotResponse(response);
  }

  return {
    render,
  };
}

let cachedRenderer = null;

function getResponseRenderer() {
  if (!cachedRenderer) {
    cachedRenderer = createResponseRenderer();
  }
  return cachedRenderer;
}

function resetResponseRendererForTests() {
  cachedRenderer = null;
}

module.exports = {
  createResponseRenderer,
  getResponseRenderer,
  resetResponseRendererForTests,
};
