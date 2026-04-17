const { assertTelegramUpdateNormalized } = require("./contracts");

function isPlainObject(value) {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function asFiniteNumber(value) {
  if (typeof value !== "number" || Number.isNaN(value) || !Number.isFinite(value)) {
    return null;
  }
  return value;
}

function asNonEmptyString(value) {
  if (value === undefined || value === null) {
    return null;
  }
  const out = String(value).trim();
  return out.length > 0 ? out : null;
}

function pickMessageEnvelope(rawUpdate) {
  if (isPlainObject(rawUpdate && rawUpdate.message)) {
    return {
      message: rawUpdate.message,
      isEdited: false,
    };
  }

  if (isPlainObject(rawUpdate && rawUpdate.edited_message)) {
    return {
      message: rawUpdate.edited_message,
      isEdited: true,
    };
  }

  return null;
}

function pickLocation(message) {
  if (!isPlainObject(message && message.location)) {
    return null;
  }

  const lat = asFiniteNumber(message.location.latitude);
  const lng = asFiniteNumber(message.location.longitude);

  if (lat === null || lng === null) {
    return null;
  }

  return { lat, lng };
}

function pickText(message) {
  return asNonEmptyString(message && message.text) || asNonEmptyString(message && message.caption) || null;
}

function normalizeTelegramUpdate(rawUpdate, { receivedAt = new Date().toISOString() } = {}) {
  if (!isPlainObject(rawUpdate)) {
    return {
      ignored: true,
      reason: "invalid_update_object",
    };
  }

  const updateId = asFiniteNumber(rawUpdate.update_id);
  if (updateId === null) {
    return {
      ignored: true,
      reason: "missing_update_id",
    };
  }

  const envelope = pickMessageEnvelope(rawUpdate);
  if (!envelope) {
    return {
      ignored: true,
      reason: "unsupported_update_type",
    };
  }

  const chatId = asFiniteNumber(envelope.message && envelope.message.chat && envelope.message.chat.id);
  const userId = asFiniteNumber(envelope.message && envelope.message.from && envelope.message.from.id);

  if (chatId === null || userId === null) {
    return {
      ignored: true,
      reason: "missing_chat_or_user",
    };
  }

  const normalized = {
    updateId,
    receivedAt,
    chatId,
    userId,
    isEdited: envelope.isEdited,
    raw: rawUpdate,
  };

  const messageId = asFiniteNumber(envelope.message && envelope.message.message_id);
  if (messageId !== null) {
    normalized.messageId = messageId;
  }

  const text = pickText(envelope.message);
  if (text) {
    normalized.text = text;
  }

  const location = pickLocation(envelope.message);
  if (location) {
    normalized.location = location;
  }

  if (!normalized.text && !normalized.location) {
    return {
      ignored: true,
      reason: "message_without_text_or_location",
    };
  }

  try {
    const validated = assertTelegramUpdateNormalized(normalized);
    return {
      ignored: false,
      value: validated,
    };
  } catch (error) {
    return {
      ignored: true,
      reason: "validation_failed",
      error: error && error.message ? error.message : String(error || "unknown_validation_error"),
    };
  }
}

module.exports = {
  normalizeTelegramUpdate,
};
