const test = require("node:test");
const assert = require("node:assert/strict");

const { createTelegramClient } = require("../src/telegramClient");

function validBotResponse(overrides = {}) {
  return {
    chatId: 123,
    text: "mensaje",
    parseMode: null,
    chunks: ["uno", "dos"],
    metadata: {
      recommendationStatus: "ok",
      usedFallback: false,
    },
    ...overrides,
  };
}

test("sendBotResponse envia todos los chunks a Telegram API", async () => {
  const calls = [];
  const fetchImpl = async (url, options) => {
    calls.push({ url, options });
    return {
      ok: true,
      status: 200,
      async json() {
        return {
          ok: true,
          result: {
            message_id: calls.length,
          },
        };
      },
    };
  };

  const client = createTelegramClient({
    token: "token_123",
    fetchImpl,
  });

  const result = await client.sendBotResponse(validBotResponse({ parseMode: "HTML" }));

  assert.equal(calls.length, 2);
  assert.match(calls[0].url, /bottoken_123\/sendMessage$/);

  const firstPayload = JSON.parse(calls[0].options.body);
  const secondPayload = JSON.parse(calls[1].options.body);

  assert.equal(firstPayload.chat_id, 123);
  assert.equal(firstPayload.text, "uno");
  assert.equal(firstPayload.parse_mode, "HTML");
  assert.equal(secondPayload.text, "dos");

  assert.equal(result.sentCount, 2);
  assert.deepEqual(result.messageIds, [1, 2]);
});

test("sendBotResponse falla con error claro cuando Telegram API responde error", async () => {
  const client = createTelegramClient({
    token: "token_123",
    fetchImpl: async () => ({
      ok: false,
      status: 500,
      async json() {
        return {
          ok: false,
          description: "internal telegram error",
        };
      },
    }),
  });

  await assert.rejects(
    () => client.sendBotResponse(validBotResponse()),
    /Telegram API sendMessage failed: internal telegram error/
  );
});
