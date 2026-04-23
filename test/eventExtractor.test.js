const test = require("node:test");
const assert = require("node:assert/strict");

const { extractEventsFromPost } = require("../src/eventExtractor");

function makeAgendaPost(ocrText) {
  return {
    postId: "post_agenda_1",
    caption: "AGENDA JUEVES 23",
    hashtags: ["cultura"],
    mentions: [],
    timestamp: "2026-04-23T00:00:00.000Z",
    childDisplayUrls: ["a", "b", "c", "d", "e"],
    ocrText,
  };
}

test("agenda OCR links title line with venue/hour line", () => {
  const post = makeAgendaPost([
    "AGENDA JUEVES 23",
    "JAM DE CANCIONES: MANU MORAN + ABBY C. + ETC",
    "CAPITAN -LADO B- / 21 HS",
    "QUARRYMEN",
    "BEATMEMO / 21 HS",
  ].join("\n"));

  const events = extractEventsFromPost(post);

  const captain = events.find((event) => event.lugar === "CAPITAN -LADO B-");
  const beatmemo = events.find((event) => event.lugar === "BEATMEMO");

  assert.ok(captain);
  assert.ok(beatmemo);

  assert.match(String(captain.payload && captain.payload.agenda_title), /JAM DE CANCIONES/i);
  assert.equal(beatmemo.payload && beatmemo.payload.agenda_title, "QUARRYMEN");
});

test("agenda OCR cleans SALIDA prefix from venue", () => {
  const post = makeAgendaPost([
    "RECORRIDA DE DISENO CON GUILLA",
    "SALIDA: VIEJO BALCON PUERTO NORTE / 10 HS",
  ].join("\n"));

  const events = extractEventsFromPost(post);
  assert.equal(events.length, 1);

  assert.equal(events[0].lugar, "VIEJO BALCON PUERTO NORTE");
  assert.equal(events[0].payload && events[0].payload.agenda_title, "RECORRIDA DE DISENO CON GUILLA");
});
