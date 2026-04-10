---
name: diagnostico-rapido
description: "Diagnosticar bugs del bot cultural con evidencia, fix minimo y validacion de regresion. Usar para incidentes de parsing, filtros, negaciones y respuestas inesperadas."
argument-hint: "Describe el bug y los pasos para reproducir"
user-invocable: true
---
# Diagnostico Rapido

## Cuando usar
- Fallas de extraccion de eventos.
- Errores en filtros por zona o audiencia.
- Negaciones mal resueltas (no quiero/sin/evitar).
- Respuestas fuera de scope cultural.
- Fallas en webhook de Apify o procesamiento de pendientes.

## Archivos clave del repo
- src/index.js (webhook /webhooks/apify/instagram)
- src/processPendingInstagramPosts.js (worker principal)
- src/eventExtractor.js (reglas de extraccion)
- src/ocr.js (OCR por imagen)
- telegramBot.py (respuesta final a usuarios)

## Procedimiento
1. Reproducir el caso con un ejemplo concreto.
2. Identificar la capa afectada (ingesta, extraccion, filtro, respuesta).
3. Formular causa raiz probable y alternativa.
4. Aplicar el cambio minimo para corregir.
5. Verificar con casos de regresion:
   - Negacion.
   - Zona sin resultados.
   - Audiencia especifica.
   - Pedido fuera de scope.

## Comandos utiles
- npm start
- npm run process:ig-posts
- npm run process:ig-posts:loop

## Criterio de cierre
- El bug original deja de reproducirse.
- No hay regresion en los cuatro casos base.
- No se exponen secretos en salida.
