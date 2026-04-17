const test = require("node:test");
const assert = require("node:assert/strict");

const { normalizeTelegramUpdate } = require("../src/telegramUpdateNormalizer");

function validTextUpdate(overrides = {}) {
  return {
    update_id: 101,
    message: {
      message_id: 202,
      from: { id: 303 },
      chat: { id: 404 },
      text: "hola",
    },
    ...overrides,
  };
}

test("normaliza message.text en TelegramUpdateNormalized valido", () => {
  const result = normalizeTelegramUpdate(validTextUpdate(), {
    receivedAt: "2026-04-17T12:00:00.000Z",
  });

  assert.equal(result.ignored, false);
  assert.equal(result.value.updateId, 101);
  assert.equal(result.value.chatId, 404);
  assert.equal(result.value.userId, 303);
  assert.equal(result.value.isEdited, false);
  assert.equal(result.value.text, "hola");
  assert.equal(result.value.receivedAt, "2026-04-17T12:00:00.000Z");
});

test("normaliza edited_message con caption y location", () => {
  const result = normalizeTelegramUpdate({
    update_id: 111,
    edited_message: {
      message_id: 222,
      from: { id: 333 },
      chat: { id: 444 },
      caption: "evento en foto",
      location: {
        latitude: -32.95,
        longitude: -60.64,
      },
    },
  });

  assert.equal(result.ignored, false);
  assert.equal(result.value.isEdited, true);
  assert.equal(result.value.text, "evento en foto");
  assert.deepEqual(result.value.location, { lat: -32.95, lng: -60.64 });
});

test("ignora update no soportado de forma limpia", () => {
  const result = normalizeTelegramUpdate({
    update_id: 123,
    callback_query: { id: "abc" },
  });

  assert.equal(result.ignored, true);
  assert.equal(result.reason, "unsupported_update_type");
});

test("ignora message sin text/caption ni location", () => {
  const result = normalizeTelegramUpdate({
    update_id: 555,
    message: {
      message_id: 777,
      from: { id: 1 },
      chat: { id: 2 },
      text: "   ",
    },
  });

  assert.equal(result.ignored, true);
  assert.equal(result.reason, "message_without_text_or_location");
});
