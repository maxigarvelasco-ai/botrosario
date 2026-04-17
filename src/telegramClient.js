const { assertBotResponse } = require("./contracts");

function asNonEmptyString(value) {
  if (value === undefined || value === null) {
    return null;
  }
  const out = String(value).trim();
  return out.length > 0 ? out : null;
}

function sanitizeBaseUrl(baseUrl) {
  const clean = asNonEmptyString(baseUrl) || "https://api.telegram.org";
  return clean.replace(/\/$/, "");
}

function createTelegramClient({
  token,
  baseUrl = "https://api.telegram.org",
  fetchImpl = global.fetch,
} = {}) {
  const cleanToken = asNonEmptyString(token);
  if (!cleanToken) {
    throw new Error("createTelegramClient requires TELEGRAM_TOKEN");
  }

  if (typeof fetchImpl !== "function") {
    throw new Error("createTelegramClient requires fetch implementation");
  }

  const cleanBaseUrl = sanitizeBaseUrl(baseUrl);

  async function callApi(method, payload) {
    const url = `${cleanBaseUrl}/bot${cleanToken}/${method}`;

    const response = await fetchImpl(url, {
      method: "POST",
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify(payload),
    });

    let body = null;
    try {
      body = await response.json();
    } catch (error) {
      body = null;
    }

    if (!response.ok) {
      const description = body && body.description ? body.description : `http_status_${response.status}`;
      throw new Error(`Telegram API ${method} failed: ${description}`);
    }

    if (!body || body.ok !== true) {
      const description = body && body.description ? body.description : "unknown_telegram_api_error";
      throw new Error(`Telegram API ${method} failed: ${description}`);
    }

    return body.result || null;
  }

  async function sendMessage({ chatId, text, parseMode = null }) {
    const cleanText = asNonEmptyString(text) || "";
    const payload = {
      chat_id: chatId,
      text: cleanText,
    };

    if (asNonEmptyString(parseMode)) {
      payload.parse_mode = parseMode;
    }

    return callApi("sendMessage", payload);
  }

  async function sendBotResponse(botResponseInput) {
    const botResponse = assertBotResponse(botResponseInput);

    const chunks = Array.isArray(botResponse.chunks) && botResponse.chunks.length > 0
      ? botResponse.chunks
      : [botResponse.text];

    const messageIds = [];
    for (const chunk of chunks) {
      const result = await sendMessage({
        chatId: botResponse.chatId,
        text: chunk,
        parseMode: botResponse.parseMode,
      });

      if (result && typeof result.message_id === "number") {
        messageIds.push(result.message_id);
      }
    }

    return {
      chatId: botResponse.chatId,
      sentCount: chunks.length,
      messageIds,
    };
  }

  return {
    sendMessage,
    sendBotResponse,
  };
}

module.exports = {
  createTelegramClient,
};
