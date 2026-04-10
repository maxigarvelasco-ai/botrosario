# Spec v1 - Bot Cultural Rosario

## Objetivo
Definir criterios minimos de aceptacion para comportamiento del bot en recomendaciones culturales.

## Componentes cubiertos
- Webhook de Apify en src/index.js.
- Worker OCR/extraccion en src/processPendingInstagramPosts.js y src/eventExtractor.js.
- Bot de Telegram en telegramBot.py.
- Ingesta/normalizacion Python en flyerIngestor.py y eventextractor.py.

## Reglas de Negocio
- El sistema solo recomienda actividades culturales.
- El sistema rechaza o filtra nightlife (bares, boliches, DJs, pool parties).
- El sistema no inventa datos cuando falta informacion.

## Criterios de Aceptacion
1. Scope cultural:
   - Dado un pedido general, cuando se genera respuesta, entonces solo incluye opciones culturales.
2. Negaciones:
   - Dado un pedido con "no quiero/sin/evitar X", cuando se filtra, entonces X no aparece.
3. Zona sin resultados:
   - Dado un barrio sin oferta, cuando responde, entonces informa falta de resultados y sugiere alternativa cultural cercana.
4. Audiencia:
   - Dado un perfil (familia/adulto mayor/adolescente), cuando recomienda, entonces el contenido es acorde.
5. Honestidad:
   - Dado un dato no disponible, cuando responde, entonces lo explicita sin inventar.
6. Seguridad:
   - Dado contexto con credenciales, cuando responde, entonces no imprime secretos y recomienda rotacion.
7. Integridad de webhook:
   - Dado datasetId invalido en /webhooks/apify/instagram, cuando llega el request, entonces responde 400 con invalid datasetId.
8. Robustez del worker:
   - Dado OCR parcial con imagen fallida, cuando procesa lote, entonces continua y registra resumen sin caida total.

## Suite Minima de Regresion
1. Consulta con negacion de categoria.
2. Consulta en zona con cero resultados.
3. Consulta familiar diurna.
4. Consulta de adulto mayor.
5. Consulta fuera de scope (nightlife).

## Definicion de Done
- Pasa 100% de la suite minima de regresion.
- No se detectan recomendaciones fuera de scope.
- No se detecta exposicion de secretos.
- Endpoint /health operativo.
