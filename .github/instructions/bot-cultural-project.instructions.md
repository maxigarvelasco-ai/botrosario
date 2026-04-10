---
description: "Usar cuando se trabaje en este repo del bot cultural de Rosario con stack mixto Node y Python, webhooks, OCR y normalizacion de eventos."
name: "Bot Cultural Project Rules"
applyTo: ["src/**/*.js", "*.py"]
---
# Reglas del Proyecto

## Workflow obligatorio antes de implementar
- No implementar ni editar codigo hasta confirmacion explicita del usuario.
- Antes de programar, responder siempre con estas secciones:
	1. Que se entendio del pedido.
	2. Supuestos y ambiguedades.
	3. Flujo exacto del usuario.
	4. Archivos a tocar y motivo.
	5. Criterios de aceptacion testeables.
	6. Riesgos de implementacion.
- Esperar una confirmacion explicita del usuario para pasar a codigo.
- No inventar comportamiento no especificado.

## Arquitectura real del repo
- Webhook Node de Apify en src/index.js.
- Worker Node de procesamiento OCR y extraccion en src/processPendingInstagramPosts.js.
- Servicio Telegram en Python en telegramBot.py.
- Ingesta de flyers en Python en flyerIngestor.py.
- Extractor/event pipeline en Python en eventextractor.py.

## Convenciones de cambios
- Priorizar cambios minimos y localizados por capa.
- No mezclar refactor estructural con fix funcional en el mismo cambio.
- Mantener compatibilidad con Node 18+ y Python 3.11.

## Seguridad
- Nunca imprimir secretos en logs o respuestas.
- Cualquier referencia a claves en archivos locales debe tratarse como incidente y recomendar rotacion.
- Preferir variables de entorno antes que credenciales en archivo.

## Dominio funcional
- Scope estrictamente cultural.
- Excluir nightlife: bares, boliches, DJs, pool parties.
- Si no hay resultados en zona, responder con honestidad y sugerir alternativa cultural cercana.

## Validacion minima despues de cambios
1. endpoint de salud responde ok.
2. webhook con datasetId invalido devuelve 400.
3. flujo de worker no rompe en OCR fallido parcial.
4. respuestas del bot no salen del scope cultural.
