# Prueba de recalada y aproximación a Exolgan

Fecha de diseño: 2026-11-22. Caso inicial: MSC CASSANDRE (IMO 9785471), ETA programada 2026-11-24 00:00 ART. El MMSI todavía no está verificado.

## Fuentes
- Transcargo: nombre, ETA y estado comercial; no confirma por sí solo la posición.
- AISStream: posiciones observadas dentro de las cajas del Río de la Plata y Dock Sud ya configuradas en scripts/update_aisstream.py.
- Servidor Nacional AIS de Prefectura: https://www.argentina.gob.ar/node/122758 . Acceso público limitado; sin API pública ni permiso de automatización verificados. No integrarlo hasta confirmación.

## Estados propuestos (a implementar, no activos)
1. SIN_POSICION_RECIENTE: no hay señal AIS reciente o no se verificó la identidad. No inferir ubicación.
2. EN_APROXIMACION: posiciones consecutivas en el Río de la Plata, con trayectoria compatible hacia zona de recalada.
3. EN_RECALADA: posiciones repetidas en una zona de fondeo definida y validada, con velocidad baja y permanencia suficiente; no basta un solo punto.
4. RUMBO_EXOLGAN: salida comprobada de la zona de recalada, posiciones consecutivas y trayectoria hacia Dock Sud; no basta un cambio de velocidad.
5. PROXIMO_EXOLGAN: trayectoria confirmada en zona de acceso a Dock Sud.
6. ARRIBADO: confirmación de atraque/arribo por fuente portuaria o evidencia independiente validada; la mera proximidad AIS no alcanza.

## Criterios de prueba
- Confirmar identidad del buque mediante MMSI verificable; nunca asignar MMSI por coincidencia aproximada de nombre.
- Registrar timestamp UTC, MMSI, latitud, longitud, velocidad cuando esté disponible, fuente y antigüedad de cada observación.
- Validar polígonos de recalada y acceso con fuentes náuticas antes de activar clasificación.
- Requerir dos o más observaciones temporalmente separadas para transiciones de movimiento; ajustar umbrales tras observar datos reales.
- Mantener el estado como no confirmado si se pierde señal; no declarar salida ni arribo por ausencia de AIS.
- Medir cobertura, falsos positivos y demoras del cron de GitHub Actions; una ventana de escucha de 120 s cada ~5 min puede perder eventos.
- Mostrar por separado ETA programada de Transcargo y ETA calculada (solo si hay datos suficientes).

## Resultado esperado
El panel de Batería Total mostrará los dos próximos barcos, estado comprobado, ETA programada, hora de última posición y antigüedad. El aviso de recalada y salida a Exolgan se activará solo cuando la prueba confirme cobertura y transiciones fiables.

Estado actual: documento de prueba; no hay implementación ni validación en vivo de estos estados.
