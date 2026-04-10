function isoNow() {
  return new Date().toISOString();
}

function safeSerializeError(error) {
  if (!error || typeof error !== "object") {
    return null;
  }

  return {
    name: error.name,
    message: error.message,
    stack: error.stack,
  };
}

function write(level, message, meta = {}, context = {}) {
  const payload = {
    ts: isoNow(),
    level,
    msg: message,
    ...context,
    ...meta,
  };

  if (payload.error instanceof Error) {
    payload.error = safeSerializeError(payload.error);
  }

  const line = JSON.stringify(payload);
  if (level === "error") {
    console.error(line);
    return;
  }
  if (level === "warn") {
    console.warn(line);
    return;
  }
  console.log(line);
}

function createLogger(context = {}) {
  return {
    child(extra = {}) {
      return createLogger({ ...context, ...extra });
    },
    info(message, meta) {
      write("info", message, meta, context);
    },
    warn(message, meta) {
      write("warn", message, meta, context);
    },
    error(message, meta) {
      write("error", message, meta, context);
    },
  };
}

module.exports = {
  createLogger,
};
