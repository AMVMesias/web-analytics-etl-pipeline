import pandas as pd
import json
import os
import re
from datetime import datetime
import ast
from tqdm import tqdm  # Para barras de progreso

# ==========================================
# CONFIGURACIÓN - MODIFICA ESTOS PARÁMETROS
# ==========================================

# Ruta del archivo CSV de entrada
file_path = 'drive/MyDrive/DataSet/Visitas_lote_02.csv'

# Nombre del archivo de salida
nombre_archivo_salida = 'visitas_expandidas_completo.csv'

# Tamaño del lote para procesamiento por partes
batch_size = 1000  # Procesar 1000 filas a la vez para mantener el uso de memoria bajo

# Indexación de hits (True: empezar desde 1, False: empezar desde 0)
indexar_desde_uno = True

# ==========================================
# FIN DE LA CONFIGURACIÓN
# ==========================================

print(f"Procesamiento completo iniciado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Archivo de entrada: {file_path}")
print(f"Archivo de salida: {nombre_archivo_salida}")
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


# Función para procesar el dataset en lotes con seguimiento del máximo de hits
def procesar_dataset_en_lotes(file_path, output_path, batch_size=1000, indexar_desde_uno=True):
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
            return 0, 0, 0

    print(f"Total de filas a procesar: {total_filas}")
    print(f"Codificación detectada: {encoding_usado}")

    # Detectar columnas JSON usando la primera fila
    try:
        primera_fila = pd.read_csv(file_path, nrows=1, encoding=encoding_usado).iloc[0]
    except Exception as e:
        print(f"Error al leer la primera fila: {e}")
        return 0, 0, 0

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

    # Para el primer lote, crearemos el archivo y escribiremos los encabezados
    first_batch = True
    total_filas_procesadas = 0

    # Para registrar todas las columnas que hemos visto
    todas_columnas = set()

    # Para rastrear el máximo número de hits en todo el dataset
    max_hits_global = 0

    # Procesar cada lote
    for i, chunk in enumerate(reader):
        print(f"\nProcesando lote {i+1} ({total_filas_procesadas}/{total_filas} filas)...")

        # Lista para almacenar filas expandidas
        filas_expandidas = []

        # Procesar cada fila en el lote actual
        for _, fila in tqdm(chunk.iterrows(), total=len(chunk), desc="Expandiendo filas"):
            fila_expandida = expandir_fila_json(fila, columnas_json, indexar_desde_uno)

            # Actualizar el máximo de hits si esta fila contiene información de hits_count
            if 'hits_count' in fila_expandida:
                hits_count = fila_expandida['hits_count']
                if hits_count > max_hits_global:
                    max_hits_global = hits_count
                    # Solo informar si es un máximo significativamente mayor
                    if max_hits_global > max_hits_global * 1.25 or max_hits_global % 10 == 0:
                        print(f"Nuevo máximo de hits detectado: {max_hits_global}")

            filas_expandidas.append(fila_expandida)

            # Actualizar el conjunto de todas las columnas
            todas_columnas.update(fila_expandida.keys())

        # Convertir a DataFrame
        df_expandido = pd.DataFrame(filas_expandidas)

        # Guardar en el archivo CSV
        if first_batch:
            df_expandido.to_csv(output_path, index=False, encoding=encoding_usado)
            first_batch = False
        else:
            df_expandido.to_csv(output_path, mode='a', header=False, index=False, encoding=encoding_usado)

        total_filas_procesadas += len(chunk)
        print(f"Progreso: {total_filas_procesadas}/{total_filas} filas ({(total_filas_procesadas/total_filas)*100:.1f}%)")
        print(f"Máximo número de hits hasta ahora: {max_hits_global}")

    print(f"\nTotal de columnas generadas: {len(todas_columnas)}")
    print(f"Máximo número de hits en todo el dataset: {max_hits_global}")

    return total_filas_procesadas, len(todas_columnas), max_hits_global

# Ejecutar el procesamiento completo
inicio = datetime.now()
print(f"Hora de inicio: {inicio.strftime('%Y-%m-%d %H:%M:%S')}")

total_filas, total_columnas, max_hits = procesar_dataset_en_lotes(
    file_path, nombre_archivo_salida, batch_size, indexar_desde_uno
)

fin = datetime.now()
tiempo_total = fin - inicio

print("\n" + "="*50)
print("ESTADÍSTICAS FINALES")
print("="*50)
print(f"Total de filas procesadas: {total_filas}")
print(f"Total de columnas generadas: {total_columnas}")
print(f"Máximo número de hits por fila: {max_hits}")
print(f"Hits indexados desde: {'1' if indexar_desde_uno else '0'}")
print(f"Tiempo de procesamiento: {tiempo_total}")

# Tamaño del archivo resultante
tamaño_mb = os.path.getsize(nombre_archivo_salida) / (1024 * 1024)
print(f"Tamaño del archivo resultante: {tamaño_mb:.2f} MB")

print("\n" + "="*50)
print("FINALIZADO")
print("="*50)
print(f"Archivo generado: {nombre_archivo_salida}")