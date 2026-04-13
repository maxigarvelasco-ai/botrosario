const startedAtMs = Date.now();

const counters = {
  httpRequestsTotal: 0,
  httpErrorsTotal: 0,
  byStatusClass: {
    "2xx": 0,
    "3xx": 0,
    "4xx": 0,
    "5xx": 0,
    other: 0,
  },
};

function statusClass(statusCode) {
  if (statusCode >= 200 && statusCode < 300) {
    return "2xx";
  }
  if (statusCode >= 300 && statusCode < 400) {
    return "3xx";
  }
  if (statusCode >= 400 && statusCode < 500) {
    return "4xx";
  }
  if (statusCode >= 500 && statusCode < 600) {
    return "5xx";
  }
  return "other";
}

function recordHttpRequest(statusCode) {
  counters.httpRequestsTotal += 1;
  const key = statusClass(Number(statusCode));
  counters.byStatusClass[key] += 1;

  if (Number(statusCode) >= 500) {
    counters.httpErrorsTotal += 1;
  }
}

function snapshot() {
  return {
    uptimeSec: Math.floor((Date.now() - startedAtMs) / 1000),
    startedAt: new Date(startedAtMs).toISOString(),
    counters: {
      httpRequestsTotal: counters.httpRequestsTotal,
      httpErrorsTotal: counters.httpErrorsTotal,
      byStatusClass: { ...counters.byStatusClass },
    },
  };
}

module.exports = {
  recordHttpRequest,
  snapshot,
};
