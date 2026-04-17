# Bot cultural: setup seguro para GitHub y Railway

Este repo ya esta preparado para usar variables de entorno y no subir secretos.

## 1) Seguridad (importante)
Las claves que compartiste quedaron expuestas en chat. Rotarlas antes de deploy:
- Supabase DB password
- APIFY_TOKEN
- GROQ_API_KEY

## 2) Archivos de configuracion
- `.gitignore`: ignora secretos y artefactos locales.
- `.env.example`: plantilla de variables.
- `requirements.txt`: dependencias minimas del servicio Telegram (Railway).
- `requirements-worker.txt`: dependencias completas para extractor/worker OCR.
- `Procfile`: comando de arranque para Railway (servicio Telegram webhook).
- `runtime.txt`: fija Python 3.11 para compatibilidad.
- `railway.toml`: start command y healthcheck explicito.

## 3) Variables en Railway
Cargar como variables de entorno (sin comillas):
- TELEGRAM_TOKEN
- GROQ_API_KEY
- SUPABASE_DATABASE_URL
- APIFY_TOKEN
- EVENTS_BUCKET
- OUT_BUCKET
- IN_BUCKET
- GOOGLE_MAPS_API_KEY (opcional)

Si Railway te provee `DATABASE_URL`, el codigo ya la toma como fallback para Postgres.

### Modelos Groq recomendados (ya configurados en `.env.example`)
- GROQ_MODEL=llama-3.3-70b-versatile
- GROQ_MODEL_FALLBACK=llama-3.3-70b-versatile
- GROQ_MODEL_EXTRACT=meta-llama/llama-4-scout-17b-16e-instruct
- GROQ_MODEL_FALLBACK_TEXT=llama-3.3-70b-versatile
- GROQ_MODEL_FALLBACK_TEXT_HARD=openai/gpt-oss-120b

Para pruebas locales con Firestore/GCS, exportar credenciales ADC:
- `GOOGLE_APPLICATION_CREDENTIALS=serviceAccountKey.json`

## 4) Deploy en Railway
1. Conectar el repo de GitHub a Railway.
2. Confirmar que detecte Python 3.11 (`runtime.txt`).
3. Confirmar comando de inicio (ya definido en `Procfile` y `railway.toml`):
   `functions-framework --source telegramBot.py --target telegram_webhook --signature-type http --port $PORT`
4. Agregar variables de entorno.
5. Deploy.

## 5) Configurar webhook de Telegram
Cuando tengas la URL publica de Railway, ejecutar:

```bash
curl "https://api.telegram.org/bot<TELEGRAM_TOKEN>/setWebhook?url=https://<TU_URL_RAILWAY>/"
```

## 6) Ejecucion local (opcional)
Con Python 3.11:

```bash
pip install -r requirements.txt
functions-framework --source telegramBot.py --target telegram_webhook --signature-type http --port 8080
```

El endpoint GET `/` responde `{"ok": true, "service": "telegram_webhook"}` para healthcheck.

Para OCR local del extractor, instalar Tesseract OCR en Windows (binario del sistema) y setear:
- `TESSERACT_LANG=spa+eng`
- `TESSERACT_CMD=C:\\Program Files\\Tesseract-OCR\\tesseract.exe` (si no esta en PATH)

## 7) Nota sobre servicios
- `telegramBot.py`: webhook HTTP para Telegram (Railway-friendly).
- `flyerIngestor.py`: webhook HTTP para payload de Apify.
- `eventextractor.py`: pensado para trigger de Cloud Storage (no HTTP puro).

`flyerIngestor.py` no se debe borrar: genera sidecar `.meta.json` que usa `eventextractor.py` para enriquecer y normalizar eventos.

## 8) Webhook Telegram en Node (nuevo)
- El server Node ahora expone webhook Telegram en `/webhooks/telegram`.
- Si definis `TELEGRAM_WEBHOOK_PATH_SECRET`, la ruta pasa a ser `/webhooks/telegram/<secret>`.
- Si definis `TELEGRAM_WEBHOOK_SECRET`, se valida el header `x-telegram-bot-api-secret-token`.
- El flujo reutiliza el use case de negocio (`telegramUseCase`) y envia chunks via Bot API.
