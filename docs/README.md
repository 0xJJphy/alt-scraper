# 📚 Centro de Documentación - Alts-Scraper

Bienvenido a la documentación técnica de Alts-Scraper. Aquí encontrarás todo lo necesario para entender el funcionamiento interno del sistema, la base de datos y cómo mantener el proyecto.

## 📖 Índice de Documentación

1.  **[Estructura del Proyecto](project_structure.md)**
    - Mapa de archivos y descripción de cada script.
    - Componentes de orquestación, scrapers y demonios.

2.  **[Base de Datos y Esquema](database.md)**
    - Diccionario de datos exhaustivo (todas las tablas y campos).
    - Frecuencias de actualización y retención de snapshots.
    - Explicación de métricas técnicas (Spread, OI, Funding, Imbalance).

3.  **[Manual de Operación (README principal)](../README.md)**
    - Guía de instalación y configuración inicial.
    - Comandos CLI y opciones de ejecución.
    - Configuración de variables de entorno.

---

## 🛠️ Guía Rápida para Desarrolladores

- **Modificar Tablas:** Edita siempre `schema.sql` y aplica los cambios.
- **Nuevo Activo:** El sistema los detecta automáticamente desde `asset_metadata`.
- **Debug:** Los logs de los demonios se pueden ver con `journalctl -u alt-scraper-realtime -f` en el VPS.
