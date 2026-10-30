# Seguimiento AIS / Exolgan — 30 de octubre de 2026

## Estado guardado
- Repositorio: `scantec77-diag/ARRIBOS`.
- Se corrigió el flujo para evitar conflictos de publicación de los datos AIS (commit `945b40410a9c722c183a23765fa404289c5490e0`).
- La ejecución `41055752106` terminó con éxito.
- La consulta de la programación de Exolgan, la conexión AISStream, la captura de 120 segundos y la publicación finalizaron correctamente.
- El resultado quedó publicado en el commit `49e832a079dac7fc262c88df9ba6c914aba70075`, que actualizó `data/ais.json` y `data/exolgan.json`.
- AIS registró 11 mensajes y 8 embarcaciones en la región; no hubo MMSI coincidentes con la lista activa de Exolgan. Por eso `data/ais.json` tiene `vessels: []` y `coverage_confirmed: false`.

## Pendiente para retomar
1. Revisar los MMSI y nombres incluidos en `data/exolgan.json` frente a los reportes AIS capturados.
2. Revisar el criterio de coincidencia de nombres/MMSI y la región/ventana de captura.
3. Ejecutar de nuevo y comprobar que las embarcaciones programadas para Exolgan aparezcan en `data/ais.json` antes de dar la cobertura por confirmada.

No se guardaron claves ni secretos en esta nota.
