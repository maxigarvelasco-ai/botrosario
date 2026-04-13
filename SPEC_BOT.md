# SPEC BOT - Formato Fijo de Features

Usar este archivo como contrato antes de implementar cualquier feature.

## Regla de proceso
- No escribir codigo hasta aprobar esta especificacion.
- La implementacion debe seguir exactamente lo aprobado.
- No agregar features extra.

## Plantilla por feature

### 1) Objetivo
- Que problema resuelve.
- Que resultado funcional se espera.

### 2) Entradas esperadas
- Frases de usuario soportadas.
- Parametros o entidades que se detectan.

### 3) Salida esperada
- Formato de respuesta.
- Campos obligatorios en la respuesta.

### 4) Reglas de negocio
- Filtros obligatorios.
- Prioridades de ordenamiento.
- Reglas de exclusion.

### 5) Datos usados
- Fuente de datos.
- Campos exactos utilizados.

### 6) Casos borde
- Sin resultados.
- Datos incompletos.
- Consulta ambigua.
- Eventos pasados/duplicados.

### 7) Criterios de aceptacion
- Lista testeable y verificable.
- Debe incluir casos positivos y negativos.

### 8) Riesgos
- Riesgos de implementacion.
- Riesgos de comportamiento incorrecto.

---

## Ejemplo: Feature "que hay hoy"

### 1) Objetivo
Responder que eventos culturales hay hoy en Rosario.

### 2) Entradas esperadas
- "que hay hoy"
- "que hay hoy en pichincha"

### 3) Salida esperada
- Maximo 5 resultados.
- Cada item con: titulo, lugar, hora, categoria.

### 4) Reglas de negocio
- Filtrar por event_date = hoy.
- Filtrar ciudad = Rosario.
- Excluir eventos ya terminados cuando haya hora.
- Ordenar por hora mas cercana.

### 5) Datos usados
- Coleccion o tabla de eventos.
- Campos: event_date, hora, ciudad, lugar, categoria, gratis.

### 6) Casos borde
- Evento sin hora.
- Consulta con zona sin resultados.
- Datos de ciudad faltantes.

### 7) Criterios de aceptacion
- No mostrar eventos de otra ciudad.
- No mostrar eventos de ayer.
- Si no hay resultados: sugerir "manana" o "este finde".

### 8) Riesgos
- Parseo incorrecto de fecha relativa.
- Ordenamiento incorrecto cuando falta hora.
