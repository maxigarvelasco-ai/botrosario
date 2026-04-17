const { normalizeTelegramUpdate } = require("./telegramUpdateNormalizer");

function createTelegramWebhookFlow({
  telegramUseCase,
  telegramClient,
  logger,
} = {}) {
  if (!telegramUseCase || typeof telegramUseCase.execute !== "function") {
    throw new Error("createTelegramWebhookFlow requires telegramUseCase.execute");
  }

  if (!telegramClient || typeof telegramClient.sendBotResponse !== "function") {
    throw new Error("createTelegramWebhookFlow requires telegramClient.sendBotResponse");
  }

  if (!logger || typeof logger.info !== "function" || typeof logger.warn !== "function" || typeof logger.error !== "function") {
    throw new Error("createTelegramWebhookFlow requires logger with info/warn/error");
  }

  async function process(rawUpdate, { requestId = null, receivedAt = new Date().toISOString() } = {}) {
    const normalizedResult = normalizeTelegramUpdate(rawUpdate, { receivedAt });
    if (normalizedResult.ignored) {
      logger.info("telegram_update_ignored", {
        requestId,
        reason: normalizedResult.reason,
      });

      return {
        ignored: true,
        reason: normalizedResult.reason,
      };
    }

    const normalizedUpdate = normalizedResult.value;
    const botResponse = await telegramUseCase.execute(normalizedUpdate);

    logger.info("telegram_use_case_executed", {
      requestId,
      chatId: normalizedUpdate.chatId,
      updateId: normalizedUpdate.updateId,
      recommendationStatus:
        botResponse && botResponse.metadata ? botResponse.metadata.recommendationStatus : null,
    });

    const sendResult = await telegramClient.sendBotResponse(botResponse);

    logger.info("telegram_messages_sent", {
      requestId,
      chatId: normalizedUpdate.chatId,
      updateId: normalizedUpdate.updateId,
      sentCount: sendResult.sentCount,
      chunksCount: Array.isArray(botResponse.chunks) ? botResponse.chunks.length : 0,
    });

    return {
      ignored: false,
      chatId: normalizedUpdate.chatId,
      updateId: normalizedUpdate.updateId,
      sentCount: sendResult.sentCount,
      recommendationStatus:
        botResponse && botResponse.metadata ? botResponse.metadata.recommendationStatus : null,
    };
  }

  return {
    process,
  };
}

module.exports = {
  createTelegramWebhookFlow,
};
