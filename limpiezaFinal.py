import pandas as pd
import numpy as np
import os
import time
import gc
import json
import logging
import csv
import traceback
from datetime import datetime
import psutil

# Configuración de logging
log_filename = f"reporte_limpieza_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger()

# Aumentar el límite de campos de CSV
csv.field_size_limit(1000000)  # Aumentar para manejar campos grandes

# Clase para manejar estadísticas y reportes
class EstadisticasLimpieza:
    def __init__(self):
        self.tiempo_inicio = time.time()
        self.filas_procesadas = 0
        self.filas_con_error = {}  # {indice_fila: mensaje_error}
        self.cambios_realizados = {}  # {tipo_cambio: cantidad}
        self.memoria_usada = []
        self.estadisticas_columnas = {}
        
    def actualizar_memoria(self):
        """Registra el uso actual de memoria"""
        mem_uso = round(self.obtener_uso_memoria_gb(), 2)
        self.memoria_usada.append(mem_uso)
        return mem_uso
    
    @staticmethod
    def obtener_uso_memoria_gb():
        """Obtiene el uso actual de memoria en GB"""
        proceso = psutil.Process(os.getpid())
        mem_info = proceso.memory_info()
        return mem_info.rss / (1024 ** 3)  # Convertir a GB
    
    def registrar_error(self, indice, error):
        """Registra un error en una fila específica"""
        self.filas_con_error[str(indice)] = str(error)
        logger.warning(f"Error en fila {indice}: {error}")
    
    def registrar_cambio(self, tipo_cambio, incremento=1):
        """Registra un tipo de cambio realizado durante la limpieza"""
        if tipo_cambio in self.cambios_realizados:
            self.cambios_realizados[tipo_cambio] += incremento
        else:
            self.cambios_realizados[tipo_cambio] = incremento
    
    def calcular_estadisticas_columna(self, df, nombre_columna):
        """Calcula estadísticas para una columna específica"""
        try:
            stats = {}
            # Para columnas numéricas
            if pd.api.types.is_numeric_dtype(df[nombre_columna]):
                stats["tipo"] = "numérica"
                stats["media"] = float(df[nombre_columna].mean()) if not df[nombre_columna].isna().all() else 0
                stats["mediana"] = float(df[nombre_columna].median()) if not df[nombre_columna].isna().all() else 0
                stats["desviacion_estandar"] = float(df[nombre_columna].std()) if not df[nombre_columna].isna().all() else 0
                stats["min"] = float(df[nombre_columna].min()) if not df[nombre_columna].isna().all() else 0
                stats["max"] = float(df[nombre_columna].max()) if not df[nombre_columna].isna().all() else 0
                stats["nulos"] = int(df[nombre_columna].isna().sum())
                stats["ceros"] = int((df[nombre_columna] == 0).sum())
            # Para columnas categóricas/texto
            else:
                stats["tipo"] = "texto/categórica"
                # Valores más frecuentes (top 5)
                value_counts = df[nombre_columna].value_counts().head(5).to_dict()
                stats["valores_frecuentes"] = {str(k): int(v) for k, v in value_counts.items()}
                stats["nulos"] = int(df[nombre_columna].isna().sum())
                stats["vacios"] = int((df[nombre_columna] == "").sum()) if df[nombre_columna].dtype == 'object' else 0
                # Longitud promedio para textos
                if df[nombre_columna].dtype == 'object':
                    stats["longitud_promedio"] = float(df[nombre_columna].fillna("").astype(str).str.len().mean())
            
            self.estadisticas_columnas[nombre_columna] = stats
        except Exception as e:
            logger.warning(f"No se pudieron calcular estadísticas para columna {nombre_columna}: {e}")
    
    def generar_reporte(self, nombre_archivo, columnas_originales, columnas_finales):
        """Genera un reporte detallado en formato JSON y texto"""
        tiempo_total = time.time() - self.tiempo_inicio
        
        # Crear reporte en JSON
        reporte = {
            "resumen": {
                "archivo_procesado": nombre_archivo,
                "tiempo_procesamiento_segundos": round(tiempo_total, 2),
                "tiempo_procesamiento_minutos": round(tiempo_total / 60, 2),
                "filas_procesadas": self.filas_procesadas,
                "filas_con_error": len(self.filas_con_error),
                "cambios_realizados": self.cambios_realizados,
                "memoria_maxima_gb": round(max(self.memoria_usada), 2) if self.memoria_usada else 0,
                "memoria_promedio_gb": round(sum(self.memoria_usada) / len(self.memoria_usada), 2) if self.memoria_usada else 0
            },
            "columnas": {
                "originales": columnas_originales,
                "finales": columnas_finales,
                "nuevas": list(set(columnas_finales) - set(columnas_originales)) if columnas_finales and columnas_originales else [],
                "eliminadas": list(set(columnas_originales) - set(columnas_finales)) if columnas_finales and columnas_originales else []
            },
            "estadisticas_columnas": self.estadisticas_columnas,
            "filas_con_error": self.filas_con_error
        }
        
        # Guardar reporte en JSON
        with open("reporte_limpieza_datos.json", "w", encoding="utf-8") as f:
            json.dump(reporte, f, ensure_ascii=False, indent=4)
            
        # Generar reporte en texto
        self._generar_reporte_texto(reporte)
        
        logger.info(f"Reporte generado y guardado como 'reporte_limpieza_datos.json' y 'reporte_limpieza_datos.txt'")
        return reporte
    
    def _generar_reporte_texto(self, reporte):
        """Genera un reporte en formato texto con la información obtenida"""
        with open("reporte_limpieza_datos.txt", "w", encoding="utf-8") as f:
            f.write("=" * 80 + "\n")
            f.write(f"REPORTE DE LIMPIEZA DE DATOS\n")
            f.write(f"Fecha: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n")
            f.write("=" * 80 + "\n\n")
            
            # Resumen
            f.write("RESUMEN DEL PROCESO\n")
            f.write("-" * 80 + "\n")
            f.write(f"Archivo procesado: {reporte['resumen']['archivo_procesado']}\n")
            f.write(f"Tiempo de procesamiento: {reporte['resumen']['tiempo_procesamiento_minutos']:.2f} minutos ({reporte['resumen']['tiempo_procesamiento_segundos']:.2f} segundos)\n")
            f.write(f"Filas procesadas: {reporte['resumen']['filas_procesadas']}\n")
            f.write(f"Filas con errores: {reporte['resumen']['filas_con_error']}\n")
            f.write(f"Memoria máxima utilizada: {reporte['resumen']['memoria_maxima_gb']:.2f} GB\n")
            f.write(f"Memoria promedio utilizada: {reporte['resumen']['memoria_promedio_gb']:.2f} GB\n\n")
            
            # Cambios realizados
            f.write("CAMBIOS REALIZADOS\n")
            f.write("-" * 80 + "\n")
            for tipo, cantidad in reporte['resumen']['cambios_realizados'].items():
                f.write(f"• {tipo}: {cantidad}\n")
            f.write("\n")
            
            # Columnas
            f.write("INFORMACIÓN DE COLUMNAS\n")
            f.write("-" * 80 + "\n")
            f.write(f"Columnas originales: {len(reporte['columnas']['originales'])}\n")
            f.write(f"Columnas finales: {len(reporte['columnas']['finales'])}\n")
            
            if reporte['columnas']['nuevas']:
                f.write("\nColumnas nuevas creadas:\n")
                for col in reporte['columnas']['nuevas']:
                    f.write(f"• {col}\n")
            
            if reporte['columnas']['eliminadas']:
                f.write("\nColumnas eliminadas:\n")
                for col in reporte['columnas']['eliminadas']:
                    f.write(f"• {col}\n")
            f.write("\n")
            
            # Estadísticas de columnas
            f.write("ESTADÍSTICAS POR COLUMNA\n")
            f.write("-" * 80 + "\n")
            column_count = 0
            for col_name, stats in reporte['estadisticas_columnas'].items():
                # Limitar número de columnas mostradas para evitar reportes excesivamente grandes
                column_count += 1
                if column_count > 20:  # Mostrar solo las primeras 20 columnas
                    f.write(f"\n... y {len(reporte['estadisticas_columnas']) - 20} columnas más.\n")
                    break
                    
                f.write(f"Columna: {col_name}\n")
                f.write(f"  Tipo: {stats['tipo']}\n")
                
                if stats['tipo'] == "numérica":
                    f.write(f"  Media: {stats.get('media', 0):.2f}\n")
                    f.write(f"  Mediana: {stats.get('mediana', 0):.2f}\n")
                    f.write(f"  Desviación estándar: {stats.get('desviacion_estandar', 0):.2f}\n")
                    f.write(f"  Mínimo: {stats.get('min', 0)}\n")
                    f.write(f"  Máximo: {stats.get('max', 0)}\n")
                    f.write(f"  Valores nulos: {stats.get('nulos', 0)}\n")
                    f.write(f"  Valores cero: {stats.get('ceros', 0)}\n")
                else:
                    f.write(f"  Valores nulos: {stats.get('nulos', 0)}\n")
                    f.write(f"  Valores vacíos: {stats.get('vacios', 0)}\n")
                    if 'longitud_promedio' in stats:
                        f.write(f"  Longitud promedio: {stats['longitud_promedio']:.2f}\n")
                    
                    f.write("  Valores más frecuentes:\n")
                    for val, count in stats.get('valores_frecuentes', {}).items():
                        val_str = str(val)
                        if len(val_str) > 50:  # Truncar valores muy largos
                            val_str = val_str[:47] + "..."
                        f.write(f"    - {val_str}: {count}\n")
                f.write("\n")
            
            # Filas con error
            if reporte['filas_con_error']:
                f.write("FILAS CON ERROR\n")
                f.write("-" * 80 + "\n")
                f.write(f"Total de filas con error: {len(reporte['filas_con_error'])}\n\n")
                
                # Mostrar solo los primeros 10 errores para no hacer el reporte demasiado largo
                count = 0
                for idx, error in reporte['filas_con_error'].items():
                    if count < 10:
                        f.write(f"Fila {idx}: {error}\n")
                        count += 1
                    else:
                        f.write(f"... y {len(reporte['filas_con_error']) - 10} errores más\n")
                        break
            
            f.write("\n" + "=" * 80 + "\n")
            f.write("FIN DEL REPORTE\n")
            f.write("=" * 80 + "\n")


# Funciones de limpieza
def limpiar_texto(texto):
    """Limpia valores de texto eliminando espacios sobrantes y caracteres problemáticos"""
    if pd.isna(texto) or not isinstance(texto, str):
        return texto
    
    # Reemplazar múltiples espacios con uno solo
    texto = ' '.join(texto.split())
    # Eliminar espacios al inicio y fin
    texto = texto.strip()
    # Normalizar caracteres especiales si es necesario
    # (aquí puedes agregar más reglas de normalización específicas)
    return texto

def corregir_formato_fecha(fecha, formatos_posibles=None):
    """Intenta corregir el formato de una fecha"""
    if pd.isna(fecha):
        return fecha
    
    if formatos_posibles is None:
        formatos_posibles = [
            '%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y', 
            '%d/%m/%Y %H:%M:%S', '%Y-%m-%d %H:%M:%S'
        ]
    
    if isinstance(fecha, str):
        for formato in formatos_posibles:
            try:
                return pd.to_datetime(fecha, format=formato)
            except:
                continue
    
    try:
        return pd.to_datetime(fecha)
    except:
        return fecha

def limpiar_lote(df, estadisticas):
    """Aplica limpieza a un lote de datos"""
    try:
        # Copiar el DataFrame para no modificar el original
        df_limpio = df.copy()
        filas_iniciales = len(df_limpio)
        
        # 1. Limpiar espacios y caracteres especiales en columnas de texto
        for columna in df_limpio.select_dtypes(include=['object']).columns:
            try:
                df_limpio[columna] = df_limpio[columna].apply(limpiar_texto)
                estadisticas.registrar_cambio(f"Limpieza de texto en columna '{columna}'")
            except Exception as e:
                estadisticas.registrar_cambio(f"Error al limpiar texto en columna '{columna}'")
                logger.error(f"Error al limpiar texto en columna '{columna}': {e}")
        
        # 2. Detectar y corregir columnas de fechas
        posibles_columnas_fecha = [col for col in df_limpio.columns if 'fecha' in col.lower() or 'date' in col.lower()]
        for columna in posibles_columnas_fecha:
            try:
                df_limpio[columna] = df_limpio[columna].apply(corregir_formato_fecha)
                estadisticas.registrar_cambio(f"Corrección de formato fecha en columna '{columna}'")
            except Exception as e:
                estadisticas.registrar_cambio(f"Error al corregir fechas en columna '{columna}'")
                logger.error(f"Error al corregir fechas en columna '{columna}': {e}")
        
        # 3. Normalizar valores numéricos (por ejemplo, comprobar que no haya caracteres no numéricos)
        for columna in df_limpio.select_dtypes(include=['number']).columns:
            try:
                # Almacenar la cantidad de valores inválidos
                valores_invalidos = pd.to_numeric(df[columna], errors='coerce').isna() & ~df[columna].isna()
                num_invalidos = valores_invalidos.sum()
                
                if num_invalidos > 0:
                    # No corregimos, solo registramos
                    estadisticas.registrar_cambio(f"Detectados {num_invalidos} valores numéricos inválidos en '{columna}'", num_invalidos)
            except Exception as e:
                estadisticas.registrar_cambio(f"Error al validar valores numéricos en columna '{columna}'")
                logger.error(f"Error al validar valores numéricos en columna '{columna}': {e}")
        
        # 4. Verificar valores atípicos o outliers
        for columna in df_limpio.select_dtypes(include=['number']).columns:
            try:
                # Calculamos Q1, Q3 y el rango intercuartílico
                q1 = df_limpio[columna].quantile(0.25)
                q3 = df_limpio[columna].quantile(0.75)
                iqr = q3 - q1
                
                # Identificamos outliers severos (no se eliminan, solo se registran)
                lower_bound = q1 - 3 * iqr
                upper_bound = q3 + 3 * iqr
                outliers = ((df_limpio[columna] < lower_bound) | (df_limpio[columna] > upper_bound))
                num_outliers = outliers.sum()
                
                if num_outliers > 0:
                    estadisticas.registrar_cambio(f"Detectados {num_outliers} outliers en columna '{columna}'", num_outliers)
            except Exception as e:
                logger.error(f"Error al detectar outliers en columna '{columna}': {e}")
        
        # 5. Convertir formatos consistentes en columnas categóricas
        for columna in df_limpio.select_dtypes(include=['object']).columns:
            # Solo procesamos columnas con pocos valores únicos (probablemente categóricas)
            if df_limpio[columna].nunique() < 20:  # Umbral arbitrario
                try:
                    # Convertimos a minúsculas y eliminamos espacios al inicio/final
                    df_limpio[columna] = df_limpio[columna].str.lower().str.strip()
                    estadisticas.registrar_cambio(f"Normalización de categorías en columna '{columna}'")
                except Exception as e:
                    logger.error(f"Error al normalizar categorías en columna '{columna}': {e}")
        
        # 6. Detectar filas con errores graves (pero no las eliminamos)
        for idx, fila in df_limpio.iterrows():
            try:
                # Verificamos si hay errores en la fila (por ejemplo, tipos de datos inconsistentes)
                for columna in df_limpio.columns:
                    # Este es un placeholder para tu lógica específica de detección de errores
                    # por ejemplo, verificar que un valor numérico sea realmente numérico
                    pass
            except Exception as e:
                estadisticas.registrar_error(idx, f"Error al procesar la fila: {e}")
        
        # 7. Calcular estadísticas para cada columna
        if estadisticas.filas_procesadas == 0:  # Solo para el primer lote
            for columna in df_limpio.columns:
                estadisticas.calcular_estadisticas_columna(df_limpio, columna)
        
        # Actualizamos estadísticas
        estadisticas.filas_procesadas += filas_iniciales
        memoria_actual = estadisticas.actualizar_memoria()
        logger.info(f"Lote procesado: {filas_iniciales} filas, memoria actual: {memoria_actual:.2f} GB")
        
        return df_limpio
        
    except Exception as e:
        logger.error(f"Error al procesar lote: {e}")
        # Devolvemos el DataFrame original sin cambios
        return df

def _procesar_batch_lineas(lineas, archivo_salida, estadisticas, columnas_originales, num_batch):
    """Procesa un lote de líneas y las escribe en el archivo de salida"""
    logger.info(f"Procesando lote de líneas #{num_batch} ({len(lineas)} líneas)")
    
    # Crear un archivo temporal para este batch
    temp_entrada = f"temp_batch_{num_batch}.csv"
    temp_salida = f"temp_batch_{num_batch}_proc.csv"
    
    try:
        # Escribir cabecera y líneas en archivo temporal
        with open(temp_entrada, 'w', encoding='utf-8', newline='') as f_temp:
            # Escribir cabecera
            f_temp.write(','.join(columnas_originales) + '\n')
            # Escribir líneas
            for linea in lineas:
                f_temp.write(linea)
        
        # Intentar procesar el archivo temporal como CSV normal
        try:
            df = pd.read_csv(temp_entrada, 
                           dtype=str,  # Todo como string para evitar errores
                           on_bad_lines='skip',
                           encoding='utf-8')
            
            # Si tiene más columnas de las esperadas, tomamos solo las originales
            if len(df.columns) > len(columnas_originales):
                logger.warning(f"Número de columnas inconsistente en lote {num_batch}: "
                              f"esperadas {len(columnas_originales)}, encontradas {len(df.columns)}")
                estadisticas.registrar_cambio(f"Inconsistencia de columnas en lote {num_batch}")
                
                # Intentar quedarnos solo con las columnas originales
                try:
                    df = df[columnas_originales]
                except Exception as e:
                    logger.error(f"No se pudieron filtrar columnas en lote {num_batch}: {e}")
            
            # Intentar limpiar datos (si es posible)
            try:
                df_limpio = limpiar_lote(df, estadisticas)
            except Exception as e:
                logger.error(f"Error al limpiar lote {num_batch}: {e}")
                df_limpio = df  # Usar DataFrame original si hay error
            
            # Guardar en archivo temporal procesado
            df_limpio.to_csv(temp_salida, index=False)
            
            # Anexar al archivo final
            with open(temp_salida, 'r', encoding='utf-8') as f_in, \
                 open(archivo_salida, 'a', encoding='utf-8') as f_out:
                next(f_in)  # Saltar cabecera
                for linea in f_in:
                    f_out.write(linea)
                    
            # Actualizar estadísticas
            estadisticas.filas_procesadas += len(df)
            
        except Exception as e:
            logger.error(f"Error al procesar lote de líneas #{num_batch} como DataFrame: {e}")
            logger.error(traceback.format_exc())
            
            # Plan B: Procesar línea por línea manualmente
            logger.info(f"Intentando procesar lote #{num_batch} línea por línea...")
            
            filas_escritas = 0
            with open(archivo_salida, 'a', encoding='utf-8') as f_out:
                for i, linea in enumerate(lineas):
                    try:
                        # Aquí podríamos añadir alguna limpieza simple a nivel de texto
                        linea_limpia = linea.strip()
                        f_out.write(linea_limpia + '\n')
                        filas_escritas += 1
                    except Exception as e2:
                        logger.error(f"Error procesando línea {i} en lote #{num_batch}: {e2}")
                        estadisticas.registrar_error(i, str(e2))
                        # Escribir línea original como fallback
                        try:
                            f_out.write(linea)
                            filas_escritas += 1
                        except:
                            pass
            
            logger.info(f"Procesamiento manual completado para lote #{num_batch}. Escritas {filas_escritas} líneas.")
            estadisticas.filas_procesadas += filas_escritas
            estadisticas.registrar_cambio(f"Procesamiento manual en lote {num_batch}")
    
    finally:
        # Limpieza de archivos temporales
        try:
            if os.path.exists(temp_entrada):
                os.remove(temp_entrada)
            if os.path.exists(temp_salida):
                os.remove(temp_salida)
        except Exception as e:
            logger.warning(f"No se pudieron eliminar archivos temporales: {e}")

def procesar_csv_manual(archivo_entrada, archivo_salida, tamano_lote=100000):
    """
    Procesa un archivo CSV grande con formato inconsistente usando lectura línea por línea
    
    Args:
        archivo_entrada: ruta al archivo CSV de entrada
        archivo_salida: ruta donde guardar el archivo CSV procesado
        tamano_lote: número aproximado de líneas a procesar por lote
    """
    logger.info(f"Iniciando procesamiento manual del archivo: {archivo_entrada}")
    
    estadisticas = EstadisticasLimpieza()
    estadisticas.cambios_realizados = {}  # Inicializar explícitamente
    
    try:
        # Verificar que el archivo existe
        if not os.path.exists(archivo_entrada):
            error_msg = f"El archivo de entrada '{archivo_entrada}' no existe"
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)
            
        # Determinar el número de líneas aproximado
        logger.info("Contando líneas del archivo (puede tardar en archivos grandes)...")
        with open(archivo_entrada, 'r', encoding='utf-8', errors='replace') as f:
            num_lineas = sum(1 for _ in f)
        logger.info(f"Total de líneas en el archivo: {num_lineas}")
        
        # Leer cabeceras
        with open(archivo_entrada, 'r', encoding='utf-8', errors='replace') as f:
            cabecera = f.readline().strip()
        
        # Parsear cabeceras usando csv module (más seguro para manejar comillas, etc)
        reader = csv.reader([cabecera])
        columnas_originales = next(reader)
        logger.info(f"Columnas detectadas: {len(columnas_originales)}")
        
        # Escribir cabecera en el archivo de salida
        with open(archivo_salida, 'w', encoding='utf-8', newline='') as f_out:
            writer = csv.writer(f_out)
            writer.writerow(columnas_originales)
        
        # Procesar archivo por lotes (líneas)
        with open(archivo_entrada, 'r', encoding='utf-8', errors='replace') as f_in:
            # Saltar cabecera
            next(f_in)
            
            lineas_batch = []
            total_procesadas = 0
            num_batch = 0
            
            logger.info("Comenzando procesamiento línea por línea...")
            for i, linea in enumerate(f_in):
                try:
                    lineas_batch.append(linea)
                    
                    # Cuando llegamos al tamaño de lote, procesamos
                    if len(lineas_batch) >= tamano_lote:
                        num_batch += 1
                        _procesar_batch_lineas(lineas_batch, archivo_salida, estadisticas, columnas_originales, num_batch)
                        total_procesadas += len(lineas_batch)
                        lineas_batch = []  # Reiniciar lote
                        
                        # Mostrar progreso
                        porcentaje = (total_procesadas / num_lineas) * 100 if num_lineas > 0 else 0
                        logger.info(f"Progreso: {total_procesadas}/{num_lineas} líneas ({porcentaje:.2f}%)")
                        
                        # Liberar memoria
                        gc.collect()
                        
                        # Verificar uso de memoria
                        memoria_actual = estadisticas.actualizar_memoria()
                        logger.info(f"Memoria después del lote {num_batch}: {memoria_actual:.2f} GB")
                
                except Exception as e:
                    logger.error(f"Error procesando línea {i+2}: {e}")  # +2 porque ya saltamos la cabecera
                    estadisticas.registrar_error(i+2, str(e))
            
            # Procesar último lote si quedaron líneas
            if lineas_batch:
                num_batch += 1
                _procesar_batch_lineas(lineas_batch, archivo_salida, estadisticas, columnas_originales, num_batch)
                total_procesadas += len(lineas_batch)
        
        logger.info(f"Total de líneas procesadas: {total_procesadas}")
        
        # Generar reporte con columnas finales = columnas originales (ya que estamos preservando la estructura)
        reporte = estadisticas.generar_reporte(archivo_entrada, columnas_originales, columnas_originales)
        return reporte
        
    except Exception as e:
        logger.error(f"Error general en el procesamiento manual: {e}")
        logger.error(traceback.format_exc())
        
        # Generar reporte parcial con la información disponible
        if 'columnas_originales' not in locals():
            columnas_originales = []
        
        if hasattr(estadisticas, 'generar_reporte'):
            reporte = estadisticas.generar_reporte(archivo_entrada, columnas_originales, columnas_originales)
            return reporte
        else:
            logger.critical("No se pudo generar el reporte final")
            return {"error": str(e)}

def procesar_csv_grande(archivo_entrada, archivo_salida, tamano_lote=100000):
    """
    Procesa un archivo CSV grande por lotes para manejar eficientemente la memoria
    
    Args:
        archivo_entrada: ruta al archivo CSV de entrada
        archivo_salida: ruta donde guardar el archivo CSV procesado
        tamano_lote: número de filas a procesar por lote
    """
    logger.info(f"Iniciando procesamiento del archivo: {archivo_entrada}")
    logger.info(f"Tamaño de lote configurado: {tamano_lote} filas")
    
    estadisticas = EstadisticasLimpieza()
    estadisticas.cambios_realizados = {}  # Inicializar explícitamente
    
    try:
        # Intentamos leer las primeras filas para obtener columnas (más seguro)
        try:
            logger.info("Intentando determinar columnas con lectura inicial...")
            df_muestra = pd.read_csv(
                archivo_entrada, 
                nrows=5,
                on_bad_lines='skip'  # Ignorar líneas problemáticas
            )
            columnas_originales = list(df_muestra.columns)
            logger.info(f"Columnas detectadas: {len(columnas_originales)}")
        except Exception as e:
            logger.warning(f"No se pudo determinar columnas automáticamente: {e}")
            logger.info("Creando lista de columnas genérica basada en la primera línea...")
            
            # Leer la primera línea manualmente para determinar columnas
            with open(archivo_entrada, 'r', encoding='utf-8', errors='replace') as f:
                primera_linea = f.readline().strip()
                columnas_originales = primera_linea.split(',')
                logger.info(f"Columnas detectadas manualmente: {len(columnas_originales)}")
        
        logger.info("Configurando lector CSV con manejo de errores...")
        # Crear un iterator sobre el archivo CSV con manejo robusto de errores
        df_iterator = pd.read_csv(
            archivo_entrada, 
            chunksize=tamano_lote,
            low_memory=False,  # Evita warnings por tipos mixtos en columnas
            encoding='utf-8',  # Ajustar según necesidades
            on_bad_lines='skip',  # Saltar líneas problemáticas
            escapechar='\\',  # Caracter de escape
            quotechar='"',    # Caracter de comillas
            doublequote=True, # Manejar dobles comillas
            skipinitialspace=True,  # Saltar espacios iniciales
        )
        
        # Procesar el primer lote y escribir con encabezados
        primer_lote = True
        columnas_finales = []
        
        try:
            for i, lote in enumerate(df_iterator):
                logger.info(f"Procesando lote {i+1} ({len(lote)} filas)")
                
                # Limpieza de lote
                try:
                    lote_limpio = limpiar_lote(lote, estadisticas)
                    columnas_finales = list(lote_limpio.columns)
                    
                    # Escribir resultados (append mode después del primer lote)
                    lote_limpio.to_csv(
                        archivo_salida, 
                        mode='w' if primer_lote else 'a',
                        header=primer_lote,
                        index=False,
                        encoding='utf-8'
                    )
                    primer_lote = False
                    
                except Exception as e:
                    logger.error(f"Error procesando lote {i+1}: {e}")
                    logger.error(traceback.format_exc())  # Registrar traza completa
                    # En caso de error, escribimos el lote original sin procesar
                    try:
                        lote.to_csv(
                            archivo_salida, 
                            mode='w' if primer_lote else 'a',
                            header=primer_lote,
                            index=False,
                            encoding='utf-8'
                        )
                        primer_lote = False
                    except Exception as e2:
                        logger.error(f"Error al escribir lote original: {e2}")
                        # Si no podemos escribir el lote, intentamos escribir una versión simplificada
                        try:
                            logger.info("Intentando escribir versión simplificada del lote...")
                            # Intentar convertir todo a string para prevenir errores
                            lote_simple = lote.astype(str)
                            lote_simple.to_csv(
                                archivo_salida, 
                                mode='w' if primer_lote else 'a',
                                header=primer_lote,
                                index=False,
                                encoding='utf-8'
                            )
                            primer_lote = False
                        except Exception as e3:
                            logger.error(f"Error al escribir versión simplificada: {e3}")
                
                # Liberar memoria
                del lote
                if 'lote_limpio' in locals():
                    del lote_limpio
                gc.collect()
                
                # Verificar uso de memoria
                memoria_actual = estadisticas.actualizar_memoria()
                logger.info(f"Memoria después del lote {i+1}: {memoria_actual:.2f} GB")
        except Exception as e:
            logger.error(f"Error durante la iteración de lotes: {e}")
            logger.error(traceback.format_exc())  # Registrar traza completa
            
        # Generar reporte
        reporte = estadisticas.generar_reporte(archivo_entrada, columnas_originales, columnas_finales)
        logger.info(f"Proceso completado. Se han procesado {estadisticas.filas_procesadas} filas.")
        logger.info(f"El archivo limpio se ha guardado como: {archivo_salida}")
        logger.info(f"El reporte de limpieza se ha guardado como: reporte_limpieza_datos.json y reporte_limpieza_datos.txt")
        
        return reporte
        
    except Exception as e:
        logger.error(f"Error general en el procesamiento: {e}")
        # Generar reporte parcial con la información disponible
        if 'columnas_originales' not in locals():
            columnas_originales = []
        if 'columnas_finales' not in locals():
            columnas_finales = columnas_originales
        
        reporte = estadisticas.generar_reporte(archivo_entrada, columnas_originales, columnas_finales)
        return reporte

if __name__ == "__main__":
    # Configuración del procesamiento
    ARCHIVO_ENTRADA = "visitas_expandidas_completo.csv"
    ARCHIVO_SALIDA = "visitas_expandidas_completo_limpio.csv"
    TAMANO_LOTE = 100000  # Ajusta según la memoria disponible
    
    # Iniciar limpieza
    logger.info("=" * 80)
    logger.info("INICIANDO PROCESO DE LIMPIEZA DE DATOS")
    logger.info("=" * 80)
    
    try:
        # Primero intentamos con el método estándar
        logger.info("Intentando procesamiento con método estándar...")
        try:
            resultado = procesar_csv_grande(ARCHIVO_ENTRADA, ARCHIVO_SALIDA, TAMANO_LOTE)
            metodo_usado = "estándar"
        except Exception as e:
            logger.warning(f"El método estándar falló: {e}")
            logger.warning(traceback.format_exc())
            logger.info("Intentando método alternativo de procesamiento línea por línea...")
            
            # Si falla, intentamos con el método manual línea por línea
            resultado = procesar_csv_manual(ARCHIVO_ENTRADA, ARCHIVO_SALIDA, TAMANO_LOTE)
            metodo_usado = "manual línea por línea"
        
        logger.info("=" * 80)
        logger.info(f"PROCESO FINALIZADO EXITOSAMENTE USANDO MÉTODO {metodo_usado.upper()}")
        logger.info("=" * 80)
        
        # Mostrar resumen final
        print("\n" + "=" * 80)
        print("RESUMEN DE LA LIMPIEZA DE DATOS")
        print("=" * 80)
        print(f"- Método de procesamiento: {metodo_usado}")
        print(f"- Archivo procesado: {ARCHIVO_ENTRADA}")
        print(f"- Archivo generado: {ARCHIVO_SALIDA}")
        
        # Mostrar estadísticas si están disponibles
        if isinstance(resultado, dict) and 'resumen' in resultado:
            print(f"- Total de filas procesadas: {resultado['resumen']['filas_procesadas']}")
            print(f"- Tiempo total: {resultado['resumen']['tiempo_procesamiento_minutos']:.2f} minutos")
            print(f"- Memoria máxima utilizada: {resultado['resumen']['memoria_maxima_gb']:.2f} GB")
            print(f"- Filas con errores: {resultado['resumen']['filas_con_error']}")
        else:
            print("- Estadísticas completas no disponibles")
            
        print(f"- Reporte detallado guardado en: reporte_limpieza_datos.txt")
        print(f"- Reporte JSON guardado en: reporte_limpieza_datos.json")
        print(f"- Log completo guardado en: {log_filename}")
        print("=" * 80)
        
    except Exception as e:
        logger.critical(f"ERROR FATAL EN EL PROCESO: {e}")
        logger.critical(traceback.format_exc())
        print(f"\nERROR: El proceso de limpieza falló. Consulte el archivo de log '{log_filename}' para más detalles.")