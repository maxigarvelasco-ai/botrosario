const test = require("node:test");
const assert = require("node:assert/strict");

const { createTelegramWebhookFlow } = require("../src/telegramWebhookFlow");

function createLoggerSpy() {
  const logs = [];

  return {
    logs,
    info(message, meta) {
      logs.push({ level: "info", message, meta });
    },
    warn(message, meta) {
      logs.push({ level: "warn", message, meta });
    },
    error(message, meta) {
      logs.push({ level: "error", message, meta });
    },
  };
}

test("process ignora update no aplicable sin ejecutar use case", async () => {
  const logger = createLoggerSpy();
  let useCaseCalled = false;
  let senderCalled = false;

  const flow = createTelegramWebhookFlow({
    logger,
    telegramUseCase: {
      async execute() {
        useCaseCalled = true;
      },
    },
    telegramClient: {
      async sendBotResponse() {
        senderCalled = true;
      },
    },
  });

  const result = await flow.process({
    update_id: 99,
    callback_query: { id: "ignored" },
  });

  assert.equal(result.ignored, true);
  assert.equal(result.reason, "unsupported_update_type");
  assert.equal(useCaseCalled, false);
  assert.equal(senderCalled, false);
});

test("process ejecuta use case y envia chunks cuando update es valido", async () => {
  const logger = createLoggerSpy();
  let receivedNormalized = null;
  let sentResponse = null;

  const flow = createTelegramWebhookFlow({
    logger,
    telegramUseCase: {
      async execute(normalizedUpdate) {
        receivedNormalized = normalizedUpdate;
        return {
          chatId: normalizedUpdate.chatId,
          text: "ok",
          parseMode: null,
          chunks: ["ok"],
          metadata: {
            recommendationStatus: "ok",
            usedFallback: false,
          },
        };
      },
    },
    telegramClient: {
      async sendBotResponse(botResponse) {
        sentResponse = botResponse;
        return {
          sentCount: 1,
        };
      },
    },
  });

  const result = await flow.process({
    update_id: 1,
    message: {
      message_id: 2,
      from: { id: 3 },
      chat: { id: 4 },
      text: "hola",
    },
  });

  assert.equal(result.ignored, false);
  assert.equal(result.chatId, 4);
  assert.equal(result.sentCount, 1);
  assert.equal(receivedNormalized.chatId, 4);
  assert.equal(sentResponse.chatId, 4);

  const sentLog = logger.logs.find((item) => item.message === "telegram_messages_sent");
  assert.ok(Boolean(sentLog));
});
