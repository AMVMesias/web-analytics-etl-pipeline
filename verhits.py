import pandas as pd
import os
import re
import json
import ast  # Para literal_eval - crucial para tu formato
import webbrowser
import traceback
from datetime import datetime
from tqdm import tqdm  # Librería para barras de progreso

# Configurar pandas para mostrar todos los datos sin truncamiento
pd.set_option('display.max_columns', None)      
pd.set_option('display.max_colwidth', None)     
pd.set_option('display.width', None)            
pd.set_option('display.max_rows', None)         

# Función de diagnóstico para examinar la estructura de hits
def examinar_estructura_hits(file_path, num_filas=5, encoding='utf-8'):
    """
    Función de diagnóstico que muestra la estructura real de los primeros ejemplos
    de hits en el dataset para depuración.
    """
    print("\n🔍 DIAGNÓSTICO DE ESTRUCTURA DE HITS")
    print("=" * 80)
    
    try:
        # Leer solo unas pocas filas para diagnóstico
        df = pd.read_csv(file_path, nrows=num_filas, encoding=encoding)
        
        if 'hits' not in df.columns:
            print("❌ No se encontró la columna 'hits' en el CSV")
            print("Columnas disponibles:", df.columns.tolist())
            return
        
        # Mostrar el tipo de datos y una muestra del contenido
        print(f"Tipo de datos de la columna 'hits': {df['hits'].dtype}")
        
        # Examinar cada fila
        for i, valor in enumerate(df['hits']):
            print(f"\nFILA {i+1}")
            print("-" * 40)
            
            if not isinstance(valor, str):
                print(f"⚠️ No es una cadena de texto: {type(valor)}")
                continue
                
            print(f"Longitud: {len(valor)} caracteres")
            print(f"Primeros 200 caracteres: {valor[:200]}...")
            
            # CLAVE: Intentar con ast.literal_eval para Python literal format
            try:
                # Usar literal_eval para evaluar el formato Python (con comillas simples)
                data_parsed = ast.literal_eval(valor)
                print("\n✅ Pudo parsearse con ast.literal_eval")
                
                # Analizar la estructura
                if isinstance(data_parsed, list):
                    print(f"  Es una lista con {len(data_parsed)} elementos")
                    
                    # Mostrar hit numbers
                    hit_numbers = []
                    for item in data_parsed:
                        if isinstance(item, dict) and 'hitNumber' in item:
                            try:
                                hit_number = int(item['hitNumber']) if isinstance(item['hitNumber'], str) else item['hitNumber']
                                hit_numbers.append(hit_number)
                            except (ValueError, TypeError):
                                print(f"  Error convirtiendo hitNumber: {item['hitNumber']}")
                    
                    print(f"  Hit numbers encontrados: {hit_numbers}")
                    if hit_numbers:
                        print(f"  Máximo hitNumber: {max(hit_numbers)}")
                    
                    # Mostrar más detalles del primer elemento
                    if data_parsed:
                        first_item = data_parsed[0]
                        print(f"\n  Primer elemento tipo: {type(first_item)}")
                        if isinstance(first_item, dict) and 'hitNumber' in first_item:
                            print(f"  'hitNumber' en primer elemento: {first_item['hitNumber']} (tipo: {type(first_item['hitNumber'])})")
                
            except Exception as e:
                print(f"\n❌ Error al intentar parsear con ast.literal_eval: {e}")
                print("  Intentando otros métodos...")
                
                # Buscar patrones con regex como respaldo
                patron_hit = r"'hitNumber':\s*'(\d+)'"
                matches = re.findall(patron_hit, valor)
                if matches:
                    print(f"  ✅ Encontrados {len(matches)} hitNumbers con regex:")
                    print(f"     Valores: {matches}")
            
            print("\n" + "=" * 80)
    
    except Exception as e:
        print(f"Error en diagnóstico: {str(e)}")
        traceback.print_exc()

# Función mejorada para determinar el máximo número de hits en el dataset
def encontrar_max_hits(file_path, sample_size=1000, encoding='utf-8'):
    print(f"\nAnalizando estructura de hits en el dataset (muestra de {sample_size} filas)...")
    
    # Primero hacer un diagnóstico detallado de las primeras filas
    examinar_estructura_hits(file_path, num_filas=5)
    
    # Leer una muestra del dataset para analizar estructura
    try:
        df_muestra = pd.read_csv(file_path, nrows=sample_size, encoding=encoding)
    except Exception as e:
        print(f"Error al leer muestra para análisis de hits: {e}")
        return 0
    
    max_hits = 0
    max_hit_number = 0
    hits_encontrados = 0
    
    # Ver si existe la columna 'hits'
    if 'hits' not in df_muestra.columns:
        print("❌ No se encontró la columna 'hits' en el CSV")
        return 0
    
    print("Analizando directamente la columna 'hits' con formato Python...")
    
    # Analizar cada fila de la muestra
    for idx, fila in tqdm(df_muestra.iterrows(), total=len(df_muestra), desc="Analizando hits"):
        try:
            if 'hits' in fila and isinstance(fila['hits'], str):
                # Método principal: usar ast.literal_eval para formato Python
                try:
                    data = ast.literal_eval(fila['hits'])
                    if isinstance(data, list):
                        num_hits = len(data)
                        
                        # Contabilizar hits y actualizar máximo
                        if num_hits > 0:
                            hits_encontrados += 1
                            max_hits = max(max_hits, num_hits)
                            
                            # Extraer hitNumbers
                            hit_numbers = []
                            for item in data:
                                if isinstance(item, dict) and 'hitNumber' in item:
                                    try:
                                        hit_num = int(item['hitNumber']) if isinstance(item['hitNumber'], str) else item['hitNumber']
                                        hit_numbers.append(hit_num)
                                    except (ValueError, TypeError):
                                        continue
                            
                            # Actualizar máximo hitNumber
                            if hit_numbers:
                                max_fila_hit = max(hit_numbers)
                                if max_fila_hit > max_hit_number:
                                    max_hit_number = max_fila_hit
                                    print(f"Nuevo máximo hitNumber: {max_hit_number} en fila {idx+1}")
                            
                            # Mostrar detalles para algunas filas
                            if (idx < 3) or (hits_encontrados <= 5):
                                print(f"\nFila {idx+1}: {num_hits} hits, hitNumbers: {hit_numbers}")
                except:
                    # Si falla ast.literal_eval, intentar con regex
                    patron_hit = r"'hitNumber':\s*'(\d+)'"
                    matches = re.findall(patron_hit, fila['hits'])
                    if matches:
                        num_hits = len(matches)
                        if num_hits > 0:
                            hits_encontrados += 1
                            max_hits = max(max_hits, num_hits)
                            
                            # Convertir a enteros
                            hit_nums = [int(m) for m in matches]
                            max_fila_hit = max(hit_nums)
                            
                            if max_fila_hit > max_hit_number:
                                max_hit_number = max_fila_hit
                                print(f"Nuevo máximo hitNumber (regex): {max_hit_number} en fila {idx+1}")
        except Exception as e:
            if idx < 10:  # Solo mostrar errores para las primeras filas
                print(f"Error en fila {idx+1}: {str(e)}")
            continue
    
    print(f"\nMáximo número de hits por fila: {max_hits}")
    print(f"Máximo hitNumber encontrado: {max_hit_number}")
    print(f"Número de filas con hits: {hits_encontrados} de {len(df_muestra)} ({hits_encontrados/len(df_muestra)*100:.2f}%)")
    return max_hits, max_hit_number

def exportar_csv_completo_a_html(csv_file=None):
    """
    Exporta todo el contenido del CSV a un archivo HTML sin omisiones ni truncamientos.
    Muestra el progreso durante la lectura del archivo y permite visualizar resultados parciales.
    """
    try:
        # Si no se proporciona un archivo, usar el predeterminado
        if csv_file is None:
            csv_file = 'Visitas_lote_02.csv'
            
        print(f"Leyendo archivo CSV: {csv_file}")
        
        # Analizar la estructura de hits para conocer el máximo global
        print("Analizando estructura de hits en el dataset...")
        max_hits_global, max_hitnumber_global_inicial = encontrar_max_hits(csv_file)
        print(f"👉 Máximo número de hits por fila detectado: {max_hits_global}")
        print(f"👉 Máximo hitNumber encontrado: {max_hitnumber_global_inicial}")
        
        # Si detectamos cero hits, preguntamos si continuar
        if max_hits_global == 0:
            continuar = input("\n⚠️ No se detectaron hits en la muestra. ¿Desea continuar con el procesamiento completo? (s/n): ").strip().lower()
            if continuar != 's':
                print("Procesamiento cancelado.")
                return None
        
        # Crear directorio de salida si no existe
        output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output')
        os.makedirs(output_dir, exist_ok=True)
        
        # Nombre del archivo HTML
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        html_file = os.path.join(output_dir, f'hits_analysis_{timestamp}.html')
        
        # Primero, contamos las líneas del archivo para la barra de progreso
        print("Calculando tamaño del archivo...")
        total_lines = sum(1 for _ in open(csv_file, 'r', encoding='utf-8'))
        print(f"Total de líneas encontradas: {total_lines}")
        
        # Leer el CSV en chunks para mostrar progreso
        chunk_size = 10000  # Ajusta según la memoria disponible
        
        # Crear el archivo HTML inicial con encabezado
        with open(html_file, 'w', encoding='utf-8') as f:
            f.write(f"""<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Análisis de Hits - {csv_file}</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 20px;
            line-height: 1.6;
        }}
        h1, h2 {{
            color: #2c3e50;
            border-bottom: 2px solid #3498db;
            padding-bottom: 10px;
        }}
        .info-panel {{
            background-color: #f8f9fa;
            border-left: 4px solid #3498db;
            padding: 10px 15px;
            margin-bottom: 20px;
        }}
        #searchBox {{
            padding: 10px;
            width: 300px;
            margin: 20px 0;
            border: 1px solid #ddd;
            border-radius: 4px;
        }}
        #progressInfo {{
            background-color: #e8f4fd;
            padding: 10px;
            margin: 15px 0;
            border-radius: 5px;
            border-left: 4px solid #2196F3;
        }}
        .summary-stats {{
            background-color: #f1f8e9;
            padding: 15px;
            margin: 20px 0;
            border-radius: 5px;
            border-left: 4px solid #8bc34a;
        }}
        .summary-item {{
            margin: 10px 0;
        }}
        .highlight {{
            font-weight: bold;
            color: #e91e63;
        }}
        table {{
            border-collapse: collapse;
            width: 100%;
            margin-top: 20px;
            font-size: 14px;
        }}
        th {{
            background-color: #3498db;
            color: white;
            position: sticky;
            top: 0;
            padding: 10px;
            text-align: left;
        }}
        td {{
            border: 1px solid #ddd;
            padding: 8px;
            word-break: break-word;
            vertical-align: top;
        }}
        tr:nth-child(even) {{
            background-color: #f2f2f2;
        }}
        tr:hover {{
            background-color: #e3f2fd;
        }}
        .footer {{
            margin-top: 30px;
            text-align: center;
            font-size: 12px;
            color: #7f8c8d;
        }}
        .hit-count {{
            font-weight: bold;
            text-align: center;
        }}
        .hit-high {{
            color: #d35400;
        }}
        .hit-med {{
            color: #2980b9;
        }}
        .hit-low {{
            color: #27ae60;
        }}
        #refreshBtn {{
            background-color: #4CAF50;
            color: white;
            padding: 8px 16px;
            border: none;
            border-radius: 4px;
            cursor: pointer;
            margin-left: 10px;
        }}
        #refreshBtn:hover {{
            background-color: #45a049;
        }}
    </style>
</head>
<body>
    <h1>Análisis de Hits - {csv_file}</h1>
    
    <div class="info-panel">
        <strong>Archivo:</strong> {csv_file}<br>
        <strong>Fecha de generación:</strong> {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}<br>
        <strong>Estado:</strong> <span id="processingStatus">Procesando...</span>
    </div>
    
    <div class="summary-stats">
        <h2>Resumen del Análisis</h2>
        <div class="summary-item">
            <strong>Máximo número de hits por fila:</strong> <span class="highlight">{max_hits_global}</span>
        </div>
        <div class="summary-item">
            <strong>Filas totales en el dataset:</strong> {total_lines-1} (excluyendo encabezado)
        </div>
        <div class="summary-item" id="filas-con-hits">
            <strong>Filas con hits:</strong> <span>Calculando...</span>
        </div>
        <div class="summary-item" id="promedio-hits">
            <strong>Promedio de hits por fila:</strong> <span>Calculando...</span>
        </div>
        <div class="summary-item" id="max-hitnumber">
            <strong>Máximo hitNumber encontrado:</strong> <span>{max_hitnumber_global_inicial}</span>
        </div>
    </div>
    
    <div id="progressInfo">
        <p>Leyendo datos del archivo... <span id="currentProgress">0</span> de aproximadamente {total_lines} filas.</p>
        <button id="refreshBtn" onclick="location.reload();">Actualizar página</button>
    </div>
    
    <input type="text" id="searchBox" placeholder="Buscar en los datos...">
    
    <table id="dataTable">
        <thead>
            <tr>
                <th>#</th>
                <th>ID de Visitante</th>
                <th>ID de Visita</th>
                <th>Hits</th>
                <th>Max HitNumber</th>
            </tr>
        </thead>
        <tbody>
""")

        # Función para extraer hitNumbers de formato Python con ast.literal_eval
        def extraer_hit_numbers(texto):
            if not isinstance(texto, str):
                return []
            
            try:
                # Usar ast.literal_eval para formato Python
                data = ast.literal_eval(texto)
                if isinstance(data, list):
                    hit_numbers = []
                    for item in data:
                        if isinstance(item, dict) and 'hitNumber' in item:
                            try:
                                hit_num = int(item['hitNumber']) if isinstance(item['hitNumber'], str) else item['hitNumber']
                                hit_numbers.append(hit_num)
                            except (ValueError, TypeError):
                                continue
                    return hit_numbers
            except:
                # Fallback a regex si ast.literal_eval falla
                patron_hit = r"'hitNumber':\s*'(\d+)'"
                matches = re.findall(patron_hit, texto)
                return [int(m) for m in matches]
            
            return []

        # Procesar el CSV en chunks con barra de progreso
        chunks = pd.read_csv(csv_file, chunksize=chunk_size)
        total_procesados = 0
        max_hitnumber_global = max_hitnumber_global_inicial
        total_hits = 0
        filas_con_hits = 0
        
        for chunk in tqdm(chunks, desc="Procesando archivo", unit="filas"):
            # Procesar cada fila
            for idx, row in chunk.iterrows():
                total_procesados += 1
                
                # Extraer información de hits si está disponible
                max_hitnumber = 0
                num_hits = 0
                
                if 'hits' in row:
                    try:
                        # Extrair hitNumbers usando la función actualizada
                        hit_numbers = extraer_hit_numbers(row['hits'])
                        num_hits = len(hit_numbers)
                        max_hitnumber = max(hit_numbers) if hit_numbers else 0
                        
                        # Actualizar estadísticas globales
                        if num_hits > 0:
                            filas_con_hits += 1
                            total_hits += num_hits
                        
                        # Actualizar máximo hitNumber global
                        if max_hitnumber > max_hitnumber_global:
                            max_hitnumber_global = max_hitnumber
                            print(f"Nuevo máximo hitNumber encontrado: {max_hitnumber_global} en fila {total_procesados}")
                        
                    except Exception as e:
                        pass
                
                # Determinar clase de estilo para hitNumber
                hit_class = "hit-low"
                if max_hitnumber > 10:
                    hit_class = "hit-high"
                elif max_hitnumber >= 5:
                    hit_class = "hit-med"
                
                # Escribir fila en HTML
                with open(html_file, 'a', encoding='utf-8') as f:
                    f.write(f'''
            <tr>
                <td>{total_procesados}</td>
                <td>{row.get('fullVisitorId', 'N/A')}</td>
                <td>{row.get('visitId', 'N/A')}</td>
                <td>{num_hits}</td>
                <td class="hit-count {hit_class}">{max_hitnumber if max_hitnumber > 0 else '-'}</td>
            </tr>''')
                
                # Actualizar el progreso en el HTML cada 1000 filas
                if total_procesados % 1000 == 0:
                    with open(html_file, 'r+', encoding='utf-8') as f:
                        content = f.read()
                        content = content.replace('<span id="currentProgress">0</span>', 
                                               f'<span id="currentProgress">{total_procesados}</span>')
                        content = content.replace('<span>Calculando...</span>', 
                                               f'<span>{filas_con_hits}</span>', 1)
                        
                        promedio = total_hits / filas_con_hits if filas_con_hits > 0 else 0
                        content = content.replace('<strong>Promedio de hits por fila:</strong> <span>Calculando...</span>', 
                                               f'<strong>Promedio de hits por fila:</strong> <span>{promedio:.2f}</span>')
                        
                        content = content.replace(f'<strong>Máximo hitNumber encontrado:</strong> <span>{max_hitnumber_global_inicial}</span>', 
                                               f'<strong>Máximo hitNumber encontrado:</strong> <span>{max_hitnumber_global}</span>')
                        
                        f.seek(0)
                        f.write(content)
                        f.truncate()
                
                # Permitir al usuario abrir el navegador después de procesar algunas filas
                if total_procesados == 500:
                    print("\n🔄 Ya se han procesado 500 filas.")
                    abrir_parcial = input("¿Desea ver los resultados parciales en el navegador? (s/n): ").strip().lower()
                    if abrir_parcial == 's':
                        webbrowser.open('file://' + os.path.abspath(html_file))
                        print("Continuando con el procesamiento...")
        
        # Calcular estadísticas finales
        promedio_hits = total_hits / filas_con_hits if filas_con_hits > 0 else 0
        
        # Finalizar el HTML
        with open(html_file, 'a', encoding='utf-8') as f:
            f.write("""
        </tbody>
    </table>
    
    <div class="summary-stats">
        <h2>Estadísticas Finales</h2>
        <div class="summary-item">
            <strong>Total de filas procesadas:</strong> """ + str(total_procesados) + """
        </div>
        <div class="summary-item">
            <strong>Filas con hits:</strong> """ + str(filas_con_hits) + """
        </div>
        <div class="summary-item">
            <strong>Total de hits:</strong> """ + str(total_hits) + """
        </div>
        <div class="summary-item">
            <strong>Promedio de hits por fila:</strong> """ + f"{promedio_hits:.2f}" + """
        </div>
        <div class="summary-item">
            <strong>Máximo número de hits por fila:</strong> """ + str(max_hits_global) + """
        </div>
        <div class="summary-item">
            <strong>Máximo hitNumber encontrado:</strong> """ + str(max_hitnumber_global) + """
        </div>
    </div>
    
    <div class="footer">
        <p>Generado automáticamente para análisis de estructura de hits</p>
    </div>
    
    <script>
        // Marcar como completado
        document.getElementById('processingStatus').textContent = 'Completado';
        document.getElementById('processingStatus').style.color = '#4CAF50';
        document.getElementById('progressInfo').style.display = 'none';
        
        // Actualizar estadísticas finales
        document.getElementById('filas-con-hits').innerHTML = 
            '<strong>Filas con hits:</strong> <span>""" + str(filas_con_hits) + """</span>';
        document.getElementById('promedio-hits').innerHTML = 
            '<strong>Promedio de hits por fila:</strong> <span>""" + f"{promedio_hits:.2f}" + """</span>';
        document.getElementById('max-hitnumber').innerHTML = 
            '<strong>Máximo hitNumber encontrado:</strong> <span>""" + str(max_hitnumber_global) + """</span>';
        
        // Función de filtrado simple
        document.getElementById('searchBox').addEventListener('keyup', function() {
            const searchValue = this.value.toLowerCase();
            const rows = document.querySelectorAll('#dataTable tbody tr');
            
            rows.forEach(row => {
                const rowText = row.textContent.toLowerCase();
                row.style.display = rowText.includes(searchValue) ? '' : 'none';
            });
        });
    </script>
</body>
</html>
""")
        
        print(f"\n✅ Archivo HTML generado exitosamente: {html_file}")
        print(f"Se procesaron {total_procesados} filas en total.")
        print(f"Estadísticas finales:")
        print(f"- Máximo número de hits por fila: {max_hits_global}")
        print(f"- Máximo hitNumber encontrado: {max_hitnumber_global}")
        print(f"- Total de hits: {total_hits}")
        print(f"- Promedio de hits por fila: {promedio_hits:.2f}")
        
        # Abrir el navegador con el archivo HTML (opcional)
        abrir_navegador = input("¿Desea abrir el archivo HTML en el navegador? (s/n): ").strip().lower()
        if abrir_navegador == 's':
            webbrowser.open('file://' + os.path.abspath(html_file))
            
        return html_file
        
    except FileNotFoundError:
        print(f"❌ Error: No se encontró el archivo '{csv_file}'")
        
        # Mostrar archivos CSV disponibles
        current_dir = os.path.dirname(os.path.abspath(__file__))
        csv_files = [f for f in os.listdir(current_dir) if f.endswith('.csv')]
        
        if csv_files:
            print("\nArchivos CSV disponibles en el directorio:")
            for i, csv_file in enumerate(csv_files, 1):
                print(f"  {i}. {csv_file}")
            
            # Permitir seleccionar un archivo
            seleccion = input("\n¿Desea seleccionar uno de estos archivos? (Ingrese el número o 'n' para cancelar): ")
            if seleccion.isdigit() and 1 <= int(seleccion) <= len(csv_files):
                archivo_seleccionado = csv_files[int(seleccion)-1]
                return exportar_csv_completo_a_html(archivo_seleccionado)
        
    except Exception as e:
        print(f"❌ Error al procesar el archivo: {str(e)}")
        traceback.print_exc()
        
    return None


if __name__ == "__main__":
    # Usar directamente el archivo deseado
    archivo = "Visitas_lote_02.csv"
    exportar_csv_completo_a_html(archivo)