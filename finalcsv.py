import pandas as pd
import json
import os
import re
from datetime import datetime
import ast
from tqdm import tqdm  # Para barras de progreso
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from IPython.display import HTML
import base64
from io import BytesIO

# ==========================================
# CONFIGURACIÓN - MODIFICA ESTOS PARÁMETROS
# ==========================================

# Ruta del archivo CSV de entrada
file_path = 'drive/MyDrive/DataSet/Visitas_lote_02.csv'

# Nombre del archivo de salida
nombre_archivo_salida = 'visitas_expandidas_completo.csv'

# Archivo para filas con demasiados hits
nombre_archivo_outliers = 'visitas_muchos_hits.csv'

# Umbral para considerar una fila como "con demasiados hits"
umbral_hits_outliers = 250  # Filas con más hits que este umbral se separarán

# Tamaño del lote para procesamiento por partes
batch_size = 1000  # Procesar 1000 filas a la vez para mantener el uso de memoria bajo

# Indexación de hits (True: empezar desde 1, False: empezar desde 0)
indexar_desde_uno = True

# ==========================================
# FIN DE LA CONFIGURACIÓN
# ==========================================

print(f"Procesamiento completo iniciado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Archivo de entrada: {file_path}")
print(f"Archivo de salida principal: {nombre_archivo_salida}")
print(f"Archivo de salida para outliers: {nombre_archivo_outliers}")
print(f"Umbral de hits para outliers: {umbral_hits_outliers}")
print(f"Indexación de hits: Comienza desde {'1' if indexar_desde_uno else '0'}")

# Función para normalizar JSON (reemplaza comillas simples por dobles)
def normalizar_json(texto_json):
    """
    Normaliza un string JSON para manejar tanto comillas simples como dobles.
    Convierte JSON con comillas simples a formato estándar con comillas dobles.
    """
    if not isinstance(texto_json, str):
        return texto_json

    try:
        # Si ya es JSON válido con comillas dobles, solo devolverlo
        json.loads(texto_json)
        return texto_json
    except:
        try:
            # Paso 1: Marcar comillas dobles existentes para preservarlas
            texto_json = texto_json.replace('\\"', '___ESCAPED_DOUBLE_QUOTE___')
            texto_json = texto_json.replace('"', '\\"')
            texto_json = texto_json.replace('___ESCAPED_DOUBLE_QUOTE___', '\\"')

            # Paso 2: Reemplazar comillas simples por dobles en pares clave-valor
            patrón = r"'([^']*)'(\s*:)"
            texto_json = re.sub(patrón, r'"\1"\2', texto_json)

            # Paso 3: Reemplazar comillas simples en valores de texto
            texto_json = texto_json.replace("\\'", "___ESCAPED_SINGLE_QUOTE___")
            texto_json = texto_json.replace("'", '"')
            texto_json = texto_json.replace("___ESCAPED_SINGLE_QUOTE___", "\\'")

            # Paso 4: Convertir booleanos Python-style a formato JSON
            texto_json = texto_json.replace('True', 'true')
            texto_json = texto_json.replace('False', 'false')
            texto_json = texto_json.replace('None', 'null')

            return texto_json
        except Exception as e:
            # En caso de error, devolver el original
            return texto_json

# Función para expandir filas JSON
def expandir_fila_json(fila, columnas_json, indexar_desde_uno=True):
    """
    Expande todas las columnas JSON de una fila y devuelve un diccionario
    con los valores expandidos, adaptándose al número de hits en cada fila.

    Args:
        fila: La fila a expandir
        columnas_json: Lista de columnas que contienen datos JSON
        indexar_desde_uno: Si es True, los hits se indexarán desde 1 (hit_1, hit_2, ...),
                          en lugar de desde 0 (hit_0, hit_1, ...)
    """
    # Crear un diccionario con los valores actuales (no-JSON)
    resultado = {}
    for col in fila.index:
        if col not in columnas_json:
            resultado[col] = fila[col]

    # Para cada columna JSON
    for columna in columnas_json:
        try:
            # Obtener el valor JSON
            valor = fila[columna]

            # CASO ESPECIAL PARA "hits": Usar conversión directa por el formato conocido
            if columna == "hits":
                # Aplicar reemplazos específicos para el formato de "hits"
                if isinstance(valor, str):
                    # Verificar si el string empieza con un formato incorrecto
                    if valor.startswith("[{") and ("True" in valor or "False" in valor):
                        # Usar ast.literal_eval para formatos Python
                        try:
                            # Primero limpiar cualquier carácter no válido al principio
                            valor_limpio = valor.strip()
                            datos = ast.literal_eval(valor_limpio)
                        except Exception:
                            # Reemplazar manualmente valores booleanos y null
                            valor = valor.replace('True', 'true').replace('False', 'false').replace('None', 'null')
                            # Eliminar cualquier carácter no válido al principio
                            valor_limpio = valor.strip()
                            try:
                                datos = json.loads(valor_limpio)
                            except Exception:
                                continue
                    else:
                        # Enfoque estándar
                        valor = valor.replace('True', 'true').replace('False', 'false').replace('None', 'null')
                        try:
                            datos = json.loads(valor)
                        except Exception:
                            continue
                else:
                    datos = valor

                # Si tenemos datos de hits en formato lista
                if isinstance(datos, list):
                    # Añadir información sobre la cantidad de hits
                    hits_count = len(datos)
                    resultado[f"{columna}_count"] = hits_count

                    # Para cada hit en la lista
                    for i, hit in enumerate(datos):
                        if isinstance(hit, dict):
                            # Crear un prefijo para este hit específico, con índice ajustado si es necesario
                            hit_index = i + 1 if indexar_desde_uno else i
                            prefix = f"{columna}_{hit_index}_"

                            # Procesar cada clave en el hit
                            for k, v in hit.items():
                                # Valores anidados: diccionarios
                                if isinstance(v, dict):
                                    for sub_k, sub_v in v.items():
                                        resultado[f"{prefix}{k}_{sub_k}"] = sub_v

                                # Valores anidados: listas
                                elif isinstance(v, list) and len(v) > 0:
                                    # Guardar longitud de la lista
                                    resultado[f"{prefix}{k}_count"] = len(v)

                                    # Para listas de diccionarios, procesar el primero
                                    if v and isinstance(v[0], dict):
                                        # Usar índice 1 o 0 según configuración
                                        item_index = 1 if indexar_desde_uno else 0
                                        for item_k, item_v in v[0].items():
                                            resultado[f"{prefix}{k}_{item_index}_{item_k}"] = item_v

                                # Valores simples
                                else:
                                    resultado[f"{prefix}{k}"] = v

                continue

            # Enfoque estándar para otras columnas
            # Normalizar JSON (manejar comillas simples)
            valor_normalizado = normalizar_json(valor)

            # Si es string, parsearlo como JSON
            if isinstance(valor_normalizado, str):
                try:
                    datos = json.loads(valor_normalizado)
                except json.JSONDecodeError:
                    continue
            else:
                datos = valor_normalizado

            # Si es un diccionario
            if isinstance(datos, dict):
                # Añadir cada clave-valor al resultado
                for k, v in datos.items():
                    resultado[f"{columna}_{k}"] = v

            # Si es una lista
            elif isinstance(datos, list) and len(datos) > 0:
                # Agregamos información sobre la longitud de la lista
                resultado[f"{columna}_list_length"] = len(datos)

                # Si los elementos son diccionarios, extraer el primer elemento
                if isinstance(datos[0], dict):
                    primer_elem = datos[0]

                    # Añadir cada clave-valor del primer elemento al resultado
                    # Usar índice 1 o 0 según configuración
                    item_index = 1 if indexar_desde_uno else 0
                    for k, v in primer_elem.items():
                        resultado[f"{columna}_item{item_index}_{k}"] = v

        except Exception:
            continue

    return resultado

# Función para determinar el máximo número de hits en el dataset

def encontrar_max_hits(file_path, sample_size=5000, encoding='utf-8'):
    print(f"\nAnalizando estructura de hits en el dataset (muestra de {sample_size} filas)...")

    # Leer una muestra del dataset para analizar estructura
    try:
        df_muestra = pd.read_csv(file_path, nrows=sample_size, encoding=encoding)
    except Exception as e:
        print(f"Error al leer muestra para análisis de hits: {e}")
        return 0, False

    max_hits = 0
    hits_count_exists = False

    # Verificar si existe la columna hits_count
    if 'hits_count' in df_muestra.columns:
        hits_count_exists = True
        print("Encontrada columna 'hits_count'. Usando esta información para determinar el máximo.")
        max_hits = df_muestra['hits_count'].max()
    else:
        print("No se encontró columna 'hits_count'. Analizando directamente la estructura JSON.")
        # Analizar cada fila de la muestra
        for _, fila in tqdm(df_muestra.iterrows(), total=len(df_muestra), desc="Analizando hits"):
            try:
                if 'hits' in fila and isinstance(fila['hits'], str):
                    valor = fila['hits']

                    # Convertir a objeto Python
                    if valor.startswith("[{") and ("True" in valor or "False" in valor):
                        try:
                            datos = ast.literal_eval(valor.strip())
                        except:
                            valor = valor.replace('True', 'true').replace('False', 'false').replace('None', 'null')
                            try:
                                datos = json.loads(valor.strip())
                            except:
                                continue
                    else:
                        try:
                            valor = valor.replace('True', 'true').replace('False', 'false').replace('None', 'null')
                            datos = json.loads(valor)
                        except:
                            continue

                    # Contar hits
                    if isinstance(datos, list):
                        max_hits = max(max_hits, len(datos))
            except:
                continue

    print(f"Máximo número de hits encontrados: {max_hits}")
    return max_hits, hits_count_exists


# Nueva función para crear un reporte visual de la distribución de hits
def generar_reporte_hits(conteo_hits, filas_outliers):
    """
    Genera un reporte HTML con un diagrama de caja y un listado de
    filas con hits extremos.

    Args:
        conteo_hits: Lista con el número de hits por fila
        filas_outliers: Lista de tuplas (índice_fila, cantidad_hits)

    Returns:
        Cadena HTML con el reporte completo
    """
    plt.figure(figsize=(10, 6))

    # Crear el diagrama de caja (boxplot)
    ax = sns.boxplot(y=conteo_hits)
    plt.title('Distribución de hits por fila')
    plt.ylabel('Número de hits')
    plt.grid(True, linestyle='--', alpha=0.7)

    # Guardar el gráfico en un buffer para convertirlo a imagen base64
    buffer = BytesIO()
    plt.savefig(buffer, format='png', bbox_inches='tight')
    buffer.seek(0)
    image_base64 = base64.b64encode(buffer.getvalue()).decode('utf-8')
    plt.close()

    # Calcular estadísticas
    stats = {
        'min': min(conteo_hits),
        'q1': np.percentile(conteo_hits, 25),
        'mediana': np.median(conteo_hits),
        'promedio': np.mean(conteo_hits),
        'q3': np.percentile(conteo_hits, 75),
        'max': max(conteo_hits),
        'desviacion': np.std(conteo_hits)
    }

    # Generar el HTML
    html = f"""
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; }}
            h1, h2 {{ color: #2c3e50; }}
            .container {{ max-width: 900px; margin: 0 auto; }}
            .stats {{ display: flex; flex-wrap: wrap; margin: 20px 0; }}
            .stat-box {{
                background-color: #f8f9fa;
                border: 1px solid #dee2e6;
                border-radius: 5px;
                padding: 10px 15px;
                margin: 5px;
                flex: 1 0 200px;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            }}
            .stat-value {{ font-size: 24px; font-weight: bold; color: #3498db; }}
            .stat-label {{ font-size: 14px; color: #7f8c8d; }}
            table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
            th, td {{ padding: 10px; text-align: left; border-bottom: 1px solid #ddd; }}
            th {{ background-color: #f2f2f2; }}
            tr:nth-child(even) {{ background-color: #f9f9f9; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Análisis de Distribución de Hits</h1>

            <div style="text-align: center; margin: 30px 0;">
                <img src="data:image/png;base64,{image_base64}" alt="Diagrama de caja" style="max-width: 100%;">
            </div>

            <h2>Estadísticas de la distribución</h2>
            <div class="stats">
                <div class="stat-box">
                    <div class="stat-value">{stats['min']}</div>
                    <div class="stat-label">Mínimo</div>
                </div>
                <div class="stat-box">
                    <div class="stat-value">{stats['q1']:.2f}</div>
                    <div class="stat-label">Primer cuartil (Q1)</div>
                </div>
                <div class="stat-box">
                    <div class="stat-value">{stats['mediana']:.2f}</div>
                    <div class="stat-label">Mediana</div>
                </div>
                <div class="stat-box">
                    <div class="stat-value">{stats['promedio']:.2f}</div>
                    <div class="stat-label">Promedio</div>
                </div>
            </div>
            <div class="stats">
                <div class="stat-box">
                    <div class="stat-value">{stats['q3']:.2f}</div>
                    <div class="stat-label">Tercer cuartil (Q3)</div>
                </div>
                <div class="stat-box">
                    <div class="stat-value">{stats['max']}</div>
                    <div class="stat-label">Máximo</div>
                </div>
                <div class="stat-box">
                    <div class="stat-value">{stats['desviacion']:.2f}</div>
                    <div class="stat-label">Desviación estándar</div>
                </div>
                <div class="stat-box">
                    <div class="stat-value">{len(filas_outliers)}</div>
                    <div class="stat-label">Filas con hits extremos</div>
                </div>
            </div>

            <h2>Filas con número extremo de hits</h2>
            <table>
                <thead>
                    <tr>
                        <th>Número de fila</th>
                        <th>Cantidad de hits</th>
                    </tr>
                </thead>
                <tbody>
    """

    for num_fila, num_hits in sorted(filas_outliers, key=lambda x: x[1], reverse=True):
        html += f"""
                    <tr>
                        <td>{num_fila}</td>
                        <td>{num_hits}</td>
                    </tr>
        """

    html += """
                </tbody>
            </table>
        </div>
    </body>
    </html>
    """

    return html

# Función para procesar el dataset en lotes con seguimiento del máximo de hits
def procesar_dataset_en_lotes(file_path, output_path, output_outliers_path, batch_size=1000,
                              umbral_hits=250, indexar_desde_uno=True):
    print(f"\nProcesando dataset completo en lotes de {batch_size} filas...")

    # Primero, contar el número total de filas con la codificación adecuada
    print("Contando filas totales...")
    try:
        # Intentar con UTF-8 primero (codificación común)
        total_filas = sum(1 for _ in open(file_path, 'r', encoding='utf-8')) - 1
        encoding_usado = 'utf-8'
    except UnicodeDecodeError:
        try:
            # Si falla, intentar con Latin-1 (que acepta cualquier byte)
            total_filas = sum(1 for _ in open(file_path, 'r', encoding='latin-1')) - 1
            encoding_usado = 'latin-1'
        except Exception as e:
            print(f"Error al contar líneas: {e}")
            return 0, 0, 0, [], []

    print(f"Total de filas a procesar: {total_filas}")
    print(f"Codificación detectada: {encoding_usado}")

    # Detectar columnas JSON usando la primera fila
    try:
        primera_fila = pd.read_csv(file_path, nrows=1, encoding=encoding_usado).iloc[0]
    except Exception as e:
        print(f"Error al leer la primera fila: {e}")
        return 0, 0, 0, [], []

    columnas_json = []

    for columna in primera_fila.index:
        valor = primera_fila[columna]
        # Convertir a string para evaluar
        valor_str = str(valor)

        # Detectar posibles valores JSON
        if isinstance(valor, str) and (
            (valor_str.startswith('{') and valor_str.endswith('}')) or
            (valor_str.startswith('[') and valor_str.endswith(']'))
        ):
            columnas_json.append(columna)

    print(f"Columnas JSON detectadas: {', '.join(columnas_json)}")

    # Inicializar lector de CSV para procesar por lotes
    reader = pd.read_csv(file_path, chunksize=batch_size, encoding=encoding_usado)

    # Para el primer lote, crearemos los archivos y escribiremos los encabezados
    first_batch_normal = True
    first_batch_outliers = True
    total_filas_procesadas = 0

    # Para registrar todas las columnas que hemos visto
    todas_columnas = set()

    # Para rastrear el máximo número de hits en todo el dataset
    max_hits_global = 0

    # Para análisis estadístico
    conteo_hits_por_fila = []
    filas_outliers = []
    fila_actual = 0  # Para llevar la cuenta del número real de fila en el dataset

    # Procesar cada lote
    for i, chunk in enumerate(reader):
        print(f"\nProcesando lote {i+1} ({total_filas_procesadas}/{total_filas} filas)...")

        # Listas para almacenar filas expandidas
        filas_expandidas_normal = []
        filas_expandidas_outliers = []

        # Procesar cada fila en el lote actual
        for _, fila in tqdm(chunk.iterrows(), total=len(chunk), desc="Expandiendo filas"):
            fila_actual += 1  # Incrementar contador de fila

            fila_expandida = expandir_fila_json(fila, columnas_json, indexar_desde_uno)

            # Verificar cantidad de hits y decidir si es un outlier
            hits_count = 0
            if 'hits_count' in fila_expandida:
                hits_count = fila_expandida['hits_count']
                conteo_hits_por_fila.append(hits_count)

                if hits_count > max_hits_global:
                    max_hits_global = hits_count

            # Separar filas con muchos hits
            if hits_count > umbral_hits:
                filas_expandidas_outliers.append(fila_expandida)
                filas_outliers.append((fila_actual, hits_count))
                print(f"Outlier detectado: Fila {fila_actual} con {hits_count} hits")
            else:
                filas_expandidas_normal.append(fila_expandida)

            # Actualizar el conjunto de todas las columnas
            todas_columnas.update(fila_expandida.keys())

        # Guardar filas normales en el archivo CSV principal
        if filas_expandidas_normal:
            df_expandido_normal = pd.DataFrame(filas_expandidas_normal)

            if first_batch_normal:
                df_expandido_normal.to_csv(output_path, index=False, encoding=encoding_usado)
                first_batch_normal = False
            else:
                df_expandido_normal.to_csv(output_path, mode='a', header=False, index=False, encoding=encoding_usado)

        # Guardar outliers en archivo separado
        if filas_expandidas_outliers:
            df_expandido_outliers = pd.DataFrame(filas_expandidas_outliers)

            if first_batch_outliers:
                df_expandido_outliers.to_csv(output_outliers_path, index=False, encoding=encoding_usado)
                first_batch_outliers = False
            else:
                df_expandido_outliers.to_csv(output_outliers_path, mode='a', header=False, index=False, encoding=encoding_usado)

        total_filas_procesadas += len(chunk)
        print(f"Progreso: {total_filas_procesadas}/{total_filas} filas ({(total_filas_procesadas/total_filas)*100:.1f}%)")
        print(f"Máximo número de hits hasta ahora: {max_hits_global}")

    print(f"\nTotal de columnas generadas: {len(todas_columnas)}")
    print(f"\nFilas separadas por exceso de hits: {len(filas_outliers)}")

    return total_filas_procesadas, len(todas_columnas), max_hits_global, conteo_hits_por_fila, filas_outliers

# Ejecutar el procesamiento completo
inicio = datetime.now()
print(f"Hora de inicio: {inicio.strftime('%Y-%m-%d %H:%M:%S')}")

total_filas, total_columnas, max_hits, conteo_hits, filas_outliers = procesar_dataset_en_lotes(
    file_path, nombre_archivo_salida, nombre_archivo_outliers, batch_size, umbral_hits_outliers, indexar_desde_uno
)

fin = datetime.now()
tiempo_total = fin - inicio

# Generar reporte HTML
if conteo_hits:
    print("\nGenerando reporte visual de la distribución de hits...")
    reporte_html = generar_reporte_hits(conteo_hits, filas_outliers)

    # Guardar el reporte HTML en un archivo
    nombre_reporte = "reporte_distribucion_hits.html"
    with open(nombre_reporte, "w", encoding="utf-8") as f:
        f.write(reporte_html)

    print(f"Reporte generado: {nombre_reporte}")

print("\n" + "="*50)
print("ESTADÍSTICAS FINALES")
print("="*50)
print(f"Total de filas procesadas: {total_filas}")
print(f"Total de columnas generadas: {total_columnas}")
print(f"Máximo número de hits por fila: {max_hits}")
print(f"Filas con hits > {umbral_hits_outliers}: {len(filas_outliers)}")
print(f"Hits indexados desde: {'1' if indexar_desde_uno else '0'}")
print(f"Tiempo de procesamiento: {tiempo_total}")

# Tamaño de los archivos resultantes
tamaño_mb_normal = os.path.getsize(nombre_archivo_salida) / (1024 * 1024)
print(f"Tamaño del archivo principal: {tamaño_mb_normal:.2f} MB")

if os.path.exists(nombre_archivo_outliers):
    tamaño_mb_outliers = os.path.getsize(nombre_archivo_outliers) / (1024 * 1024)
    print(f"Tamaño del archivo de outliers: {tamaño_mb_outliers:.2f} MB")

print("\n" + "="*50)
print("FILAS CON NÚMERO EXTREMO DE HITS")
print("="*50)
for num_fila, num_hits in sorted(filas_outliers, key=lambda x: x[1], reverse=True)[:10]:  # Mostrar los 10 casos más extremos
    print(f"Fila {num_fila}: {num_hits} hits")

if len(filas_outliers) > 10:
    print(f"... y {len(filas_outliers) - 10} filas más")

print("\n" + "="*50)
print("FINALIZADO")
print("="*50)
print(f"Archivo principal generado: {nombre_archivo_salida}")
print(f"Archivo de outliers generado: {nombre_archivo_outliers}")
print(f"Reporte HTML generado: {nombre_reporte}")