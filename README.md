<p align="center">
  <img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python"/>
  <img src="https://img.shields.io/badge/Pandas-150458?style=for-the-badge&logo=pandas&logoColor=white" alt="Pandas"/>
  <img src="https://img.shields.io/badge/NumPy-013243?style=for-the-badge&logo=numpy&logoColor=white" alt="NumPy"/>
  <img src="https://img.shields.io/badge/Power_BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black" alt="PowerBI"/>
</p>

# 📊 Pipeline ETL — Web Analytics

Pipeline ETL de alto rendimiento construido en Python, diseñado para procesar y limpiar datasets masivos de analítica web (7 GB+). Maneja expansión de JSON, normalización de datos, detección de outliers y generación de reportes visuales — todo optimizado para entornos con memoria limitada.

## 🎯 Problema

Los datos crudos de herramientas como Google Analytics se exportan como archivos CSV con **columnas JSON anidadas**. Cada fila representa una visita de usuario, y los campos JSON contienen arrays de hits de páginas, interacciones con productos y datos de fuentes de tráfico.

**Desafíos:**
- Archivos de **7 GB+** — no caben en memoria
- Columnas JSON con **arrays de longitud variable** (1 a 250+ hits por visita)
- Datos con **formato inconsistente**, problemas de encoding y JSON malformado
- Necesidad de aplanar, limpiar y preparar para dashboards en PowerBI

## 🚀 Solución

El pipeline procesa datos en **lotes configurables**, usando I/O streaming y monitoreo de memoria para manejar archivos de cualquier tamaño.

```
CSV Crudo (7 GB+)        CSV Limpio y Expandido          Dashboard PowerBI
┌──────────────┐         ┌──────────────────┐            ┌──────────────┐
│ visit_id     │         │ visit_id         │            │  📈 Gráficos │
│ hits (JSON)  │  ────►  │ hit_1_page       │   ────►    │  📊 KPIs     │
│ traffic_src  │         │ hit_1_time       │            │  🗺️ Mapas    │
│ device (JSON)│         │ hit_2_page       │            │  📋 Tablas   │
│ ...          │         │ device_browser   │            └──────────────┘
└──────────────┘         │ traffic_source   │
                         │ ...              │
                         └──────────────────┘
```

## 📁 Estructura del Proyecto

```
web-analytics-etl-pipeline/
│
├── finalcsv.py            # Paso 1: Expansión JSON + detección de outliers + reporte HTML
├── limpiezaFinal.py       # Paso 2: Limpieza y normalización del CSV expandido
│
├── sample_data.csv        # Muestra de entrada (5 filas de referencia)
├── requirements.txt       # Dependencias Python
├── LICENSE
└── README.md
```

## ⚙️ Componentes del Pipeline

### Paso 1: `finalcsv.py` — Expansión JSON
- Lee el CSV en lotes configurables (por defecto: 1,000 filas)
- Detecta y normaliza columnas JSON (maneja comillas simples/dobles)
- Expande arrays de hits de longitud variable en columnas planas (`hit_1_page`, `hit_2_page`, ...)
- **Detección de outliers**: Separa filas con conteos de hits anormalmente altos en un archivo aparte
- **Reporte HTML**: Visualización interactiva de la distribución de hits
- **Output:** `visitas_expandidas_completo.csv` + `visitas_muchos_hits.csv` + reporte HTML

### Paso 2: `limpiezaFinal.py` — Limpieza de Datos
Lee el CSV expandido del Paso 1 y aplica limpieza profunda:
- **Clase `EstadisticasLimpieza`**: Rastreo de todas las métricas
- **Monitoreo de memoria**: Uso de RAM en tiempo real con `psutil`
- **Corrección de formatos de fecha**: Maneja múltiples formatos
- **Normalización de texto**: Limpieza de whitespace y encoding
- **Generación de reportes**: JSON + texto formateado
- **Procesamiento resiliente**: Fallback línea por línea para CSVs malformados
- **Output:** `visitas_expandidas_completo_limpio.csv` (listo para Power BI)

## 🔧 Uso

### Instalación

```bash
pip install -r requirements.txt
```

### Ejecutar el Pipeline

```bash
# Paso 1: Expansión JSON (genera visitas_expandidas_completo.csv)
python finalcsv.py

# Paso 2: Limpieza de datos (genera visitas_expandidas_completo_limpio.csv)
python limpiezaFinal.py
```

> **Nota:** Configurar las rutas de archivos de entrada/salida al inicio de cada script antes de ejecutar.

## 📈 Métricas Clave

| Métrica | Valor |
|---------|-------|
| **Tamaño del archivo** | 7.6 GB |
| **Filas procesadas** | 1,000,000+ |
| **Máximo hits por visita** | 250+ |
| **Modo de procesamiento** | Por lotes (configurable) |
| **Monitoreo de memoria** | Tiempo real (psutil) |
| **Formatos de salida** | CSV, HTML, JSON, TXT |

## 🛠️ Tecnologías

| Tecnología | Uso |
|---|---|
| **Python 3.8+** | Lenguaje principal |
| **Pandas** | Manipulación de datos e I/O por lotes |
| **NumPy** | Operaciones numéricas |
| **Matplotlib + Seaborn** | Visualizaciones estadísticas |
| **psutil** | Monitoreo de memoria y recursos |
| **tqdm** | Barras de progreso |
| **Power BI** | Visualización final en dashboards |

## 📝 Licencia

Este proyecto está bajo la Licencia MIT — ver el archivo [LICENSE](LICENSE) para más detalles.
