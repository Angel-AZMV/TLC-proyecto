# TLC Proyecto — Pipeline de Datos NYC Taxi

Proyecto de arquitectura medallion (bronze → silver → gold) sobre datos públicos de la NYC Taxi & Limousine Commission (TLC), con dashboards en Power BI y modelos predictivos (series de tiempo, clustering, clasificación).

## Estructura del repositorio

```
├── notebooks/
│   └── 01_eda_bronze.ipynb               # Exploración de datos crudos (no productivo, opcional)
├── pipelines/                             # Pipeline productivo, ejecutar en este orden
│   ├── 01_download.py                    # Descarga de datos crudos NYC TLC (script, no notebook)
│   ├── 02_bronze_to_silver_v2.ipynb      # Limpieza y estandarización
│   ├── 03_silver_to_gold.ipynb           # Tablas descriptivas y diagnósticas
│   └── 04_silver_to_gold_predictivo.ipynb # Modelos predictivos (Prophet)
├── src/                                   # Módulos reutilizables
│   ├── config.py                         # Rutas y constantes del proyecto
│   ├── spark_utils.py                    # Creación de sesión Spark
│   ├── silver/
│   │   ├── cleaning_rules.py             # Reglas de limpieza aplicadas en bronze -> silver
│   │   ├── schemas.py                    # Esquemas esperados por tipo de vehículo
│   │   └── transform.py                  # Transformaciones bronze -> silver
│   └── gold/
│       ├── dimensions.py                 # dim_tiempo, dim_ubicacion
│       ├── descriptivo.py                # viajes_por_hora, viajes_por_zona, tendencia_mensual
│       ├── diagnostico.py                # (deprecado, ver notas abajo)
│       └── predictivo.py                 # features_serie_tiempo, forecast_demanda
├── docs/                                  # Documentación adicional
└── data/
    ├── dims/                             # taxi_zones.csv (fuente para dim_ubicacion)
    └── gold/                             # Tablas Gold ya generadas (incluidas en el repo)
```

> `pipelines/` contiene el flujo productivo real (ejecutar en orden 01→04). `notebooks/` es solo para exploración de datos (EDA), no forma parte del pipeline.

> **Nota:** `data/bronze/` y `data/silver/trips/` **no están incluidas** en este repositorio por su tamaño (~30GB). Se regeneran localmente siguiendo los pasos de abajo.

---

## Opción rápida: usar `gold` ya generado

Si solo necesitas trabajar con Power BI o los notebooks de modelos predictivos, **no necesitas reproducir todo el pipeline**. La carpeta `data/gold/` ya viene incluida en este repo — con clonar es suficiente:

```bash
git clone https://github.com/Angel-AZMV/TLC-proyecto.git
cd TLC-proyecto
```

Los archivos parquet de `data/gold/` quedan listos para abrir en Power BI o para correr `04_silver_to_gold_predictivo.ipynb`.

---

## Cómo levantar el entorno (Docker)

El proyecto corre sobre un cluster Spark + Jupyter definido en `docker-compose.yml`:

- `spark` — nodo master (imagen `apache/spark:3.5.1-python3`)
- `spark-worker` — nodo worker, 4GB RAM / 2 cores
- `jupyter` — Jupyter con PySpark (imagen `jupyter/pyspark-notebook`), monta `data/`, `pipelines/`, `notebooks/`, `docs/`, `src/`

### Requisito previo: Docker instalado

Necesitas **Docker Desktop** (Windows/Mac) o **Docker Engine + Docker Compose** (Linux). Verifica que ya lo tengas instalado:

```bash
docker --version
docker compose version
```

Si da error de "comando no encontrado", instala Docker Desktop desde [docker.com](https://www.docker.com/products/docker-desktop/) antes de continuar.

### Paso 1 — Ubícate en la raíz del proyecto

El `docker-compose.yml` usa rutas relativas (`./data`, `./pipelines`, etc.), así que debes estar parado en la carpeta que contiene tanto el archivo como las carpetas `data/`, `pipelines/`, `notebooks/`, `docs/`, `src/`:

```bash
cd ruta/a/tu/TLC-proyecto
ls
```

Deberías ver `docker-compose.yml` al mismo nivel que esas carpetas.

### Paso 2 — Levanta los contenedores

```bash
docker compose up -d
```

El flag `-d` ("detached") corre todo en segundo plano y te devuelve la terminal. La primera vez va a descargar las imágenes (`apache/spark:3.5.1-python3`, `jupyter/pyspark-notebook`), lo cual puede tardar varios minutos según tu conexión — las siguientes veces es casi instantáneo porque las imágenes ya quedan en caché local.

Esto levanta 3 contenedores: `tlc_spark` (master), `tlc_spark_worker` (worker), `tlc_jupyter`.

### Paso 3 — Verifica que los 3 contenedores estén corriendo

```bash
docker compose ps
```

Los 3 deben mostrar estado `Up` (o `running`). Si alguno aparece como `Exited` o `Restarting`, revisa su log específico para diagnosticar:

```bash
docker compose logs spark
docker compose logs spark-worker
docker compose logs jupyter
```

### Paso 4 — Obtén el link de acceso a Jupyter

Jupyter genera un token de acceso al iniciar. Búscalo en su log:

```bash
docker compose logs jupyter | grep token
```

Vas a ver una línea similar a:
```
http://127.0.0.1:8888/lab?token=abc123...
```

Copia esa URL completa y ábrela en el navegador — entra directo a Jupyter Lab con acceso a todas las carpetas del proyecto ya montadas dentro de `/home/jovyan/`.

### Paso 5 — Instala las dependencias extra (una sola vez por contenedor)

La imagen base `jupyter/pyspark-notebook` no incluye `prophet`, `duckdb` ni `scikit-learn` con las versiones exactas usadas en el desarrollo:

```bash
docker compose exec jupyter pip install -r requirements.txt --break-system-packages
```

Si en algún momento recreas el contenedor desde cero (`docker compose down` seguido de `up` con `--build` o borrando el contenedor), este paso hay que repetirlo — las librerías instaladas con `pip` no persisten fuera del contenedor a menos que se agreguen a una imagen personalizada.

### Paso 6 — Verifica que Spark esté accesible (opcional)

Abre en el navegador `http://localhost:8080` — deberías ver la interfaz web del Spark Master, con 1 worker conectado (4GB RAM, 2 cores). Sirve para confirmar visualmente que el cluster está sano antes de correr cualquier notebook pesado.

### Apagar el entorno

```bash
docker compose down
```

Apaga los 3 contenedores. **Los datos no se pierden** — `data/`, `pipelines/`, `notebooks/`, `docs/` y `src/` están montados desde tu carpeta local (bind mount), no viven dentro del contenedor. La próxima vez que corras `docker compose up -d`, todo sigue exactamente donde lo dejaste.

### Problemas comunes

| Síntoma | Causa probable | Solución |
|---|---|---|
| `port is already allocated` al hacer `up` | Otro proceso ya usa el puerto 8888, 8080 o 7077 | Cierra el proceso que ocupa el puerto, o cambia el mapeo de puertos en `docker-compose.yml` (ej. `"8889:8888"`) |
| `docker compose ps` muestra `spark-worker` como `Exited` | El worker no logró conectar al master (a veces por orden de arranque) | `docker compose restart spark-worker` |
| Jupyter pide token/contraseña y no lo tienes | No copiaste la URL completa con el token del log | Repite el Paso 4, o corre `docker compose exec jupyter jupyter server list` para ver el token activo |
| `ModuleNotFoundError` en Prophet/duckdb/sklearn dentro de un notebook | Falta el Paso 5 | Corre `docker compose exec jupyter pip install -r requirements.txt --break-system-packages` |

---

## Reproducir el pipeline (dentro del contenedor Jupyter)

Necesario solo si vas a modificar la lógica de bronze→silver→gold. Asegúrate de haber completado el Paso 5 de la sección anterior (dependencias instaladas) antes de correr cualquiera de estos.

### Paso 1 — Descargar datos crudos (bronze)

Script con argumentos por línea de comandos. **Nota:** el flag `--year` no filtra la descarga — el script siempre usa el rango de años definido en `ANIOS` (`src/config.py`), independientemente del valor pasado. Solo `--dtype` es necesario para elegir el tipo de vehículo:

```bash
python pipelines/01_download.py --dtype yellow --workers 4
python pipelines/01_download.py --dtype green --workers 4
python pipelines/01_download.py --dtype fhv --workers 4
python pipelines/01_download.py --dtype fhvhv --workers 4
```

Cada ejecución descarga automáticamente todos los años configurados para ese tipo de vehículo. `--workers` controla la descarga en paralelo (ajustable según tu conexión). El script detecta archivos ya descargados y los omite (`[SKIP] Ya existe: ...`), así que se puede interrumpir y volver a correr sin duplicar trabajo.

Descarga los archivos parquet mensuales de NYC TLC directo desde la fuente oficial (`https://d37ci6vzurychx.cloudfront.net/trip-data`), guardados como `data/bronze/{tipo}/{tipo}_tripdata_YYYY-MM.parquet`.

**Fuente de datos:** https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

### Paso 2 — Bronze a Silver

```bash
jupyter nbconvert --to notebook --execute pipelines/02_bronze_to_silver_v2.ipynb
```

Limpieza, tipado, y estandarización de columnas (lógica en `src/silver/cleaning_rules.py`, `schemas.py` y `transform.py`). Particiona por `tipo_vehiculo` y `anio` (con subpartición `mes` según el tipo).

> El pipeline procesa archivo por archivo (no une todo en memoria) por límite de RAM del entorno, e incluye reintento automático ante fallos de memoria de Spark. También tiene resume automático: si ya procesó una combinación tipo/año/mes, la salta. Es normal ver mensajes de `[SKIP]` o reintentos en el log — no son errores.

### Paso 3 — Silver a Gold (descriptivo)

```bash
jupyter nbconvert --to notebook --execute pipelines/03_silver_to_gold.ipynb
```

Construye:
- `gold/dims/dim_tiempo`, `gold/dims/dim_ubicacion` (desde `data/dims/taxi_zones.csv`)
- `gold/descriptivo/viajes_por_hora`, `viajes_por_zona`, `tendencia_mensual`

### Paso 4 — Silver a Gold (predictivo)

```bash
jupyter nbconvert --to notebook --execute pipelines/04_silver_to_gold_predictivo.ipynb
```

Construye `gold/predictivo/features_serie_tiempo` y entrena modelos Prophet (con feriados de EE. UU. como regresor) para los 4 tipos de vehículo, exportando el forecast a `gold/predictivo/forecast_demanda`.

### Opcional — Exploración de datos

```bash
jupyter nbconvert --to notebook --execute notebooks/01_eda_bronze.ipynb
```

No es parte del pipeline productivo; útil solo para explorar visualmente los datos crudos.

---

## Modelo de datos (Power BI)

El modelo es una **constelación de hechos** (fact constellation), no un star schema simple: varias tablas de hechos (`viajes_por_zona`, `viajes_por_hora`, `tendencia_mensual`) comparten dimensiones conformadas (`dim_tiempo`, `dim_ubicacion`). Cada fact table tiene un grano distinto, intencional según la pregunta de negocio que responde. `tipo_vehiculo` se maneja como dimensión degenerada dentro de cada fact table.

---

## Notas de decisiones tomadas

- **Outlier de yellow (22–24 sep 2023):** se excluyó de la serie de tiempo usada para entrenar Prophet por evidencia de falla de ingesta específica del proveedor (confirmado por comparación cruzada contra los demás tipos de vehículo, que no mostraron la misma caída).
- **`diagnostico.py`** (`comparacion_vehiculos`, `duracion_distancia`, `tarifas_por_zona`) quedó deprecado por redundancia con `tendencia_mensual`; no se usa en el modelo final de Power BI.

---

## Equipo

Repositorio del proyecto de curso — arquitectura de datos con PySpark, Power BI y modelos predictivos sobre datos de NYC TLC.
