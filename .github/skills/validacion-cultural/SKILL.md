---
name: validacion-cultural
description: "Validar respuestas del bot contra reglas de contenido cultural, negaciones y honestidad. Usar antes de deploy o luego de cambios en logica de recomendacion."
argument-hint: "Pasa un lote de consultas y respuestas para validar"
user-invocable: true
---
# Validacion Cultural

## Objetivo
Asegurar que las respuestas cumplan el alcance cultural y no inventen informacion.

## Alcance tecnico en este repo
- Validar salida final de telegramBot.py.
- Verificar que la extraccion en src/eventExtractor.js no abra categorias fuera de scope.
- Confirmar que los pipelines de ingesta (src/index.js y src/processPendingInstagramPosts.js) no degraden datos clave de fecha, lugar y ciudad.

## Checklist
1. Scope cultural estricto.
2. Sin bares/boliches/DJs/nightlife.
3. Negaciones respetadas.
4. Si no hay resultados por zona, respuesta honesta + alternativa cultural cercana.
5. Audiencia respetada cuando se solicita.
6. Tono claro, sin sobrepromesas.

## Resultado esperado
- Clasificacion por consulta: OK o NO OK.
- Motivo breve por cada NO OK.
- Recomendacion puntual de correccion.

## Lote minimo recomendado
- 3 consultas con negacion explicita.
- 2 consultas por zona con y sin resultados.
- 2 consultas por audiencia (familiar y adulto mayor).
- 2 consultas fuera de scope para verificar bloqueo.
