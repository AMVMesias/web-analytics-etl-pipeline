import pandas as pd
import webbrowser
import os
from datetime import datetime

# Ruta al archivo CSV
archivo_csv = 'visitas_expandidas_completo2.csv'

# Crear timestamp de ejecución
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

try:
    print("Cargando datos...")
    
    # Cargar solo las primeras 5 filas - esto es rápido y debería funcionar siempre
    primeras_filas = pd.read_csv(
        archivo_csv, 
        nrows=5,
        low_memory=False,
        on_bad_lines='skip'
    )
    
    # Calcular número de columnas en las primeras filas
    num_columnas_inicio = primeras_filas.shape[1]
    print(f"Número de columnas en las primeras filas: {num_columnas_inicio}")
    
    # Aproximación simple para las últimas filas
    # Este método es menos preciso pero más robusto
    try:
        # Obtener tamaño del archivo
        tamanio_archivo = os.path.getsize(archivo_csv)
        
        # Para archivos muy grandes, leer un fragmento del final
        if tamanio_archivo > 10_000_000:  # Si es mayor a 10MB
            # Leer los últimos ~1MB del archivo
            CHUNK_SIZE = 1_000_000
            with open(archivo_csv, 'r', encoding='utf-8', errors='ignore') as f:
                f.seek(max(0, tamanio_archivo - CHUNK_SIZE))
                # Descartar la primera línea incompleta
                f.readline()
                # Leer el resto
                ultimas_lineas = f.read()
            
            # Guardar en un archivo temporal
            temp_file = f"temp_{timestamp}.csv"
            with open(temp_file, 'w', encoding='utf-8') as f:
                # Incluir encabezados del CSV original
                encabezados = pd.read_csv(archivo_csv, nrows=0).columns.tolist()
                f.write(','.join(encabezados) + '\n')
                # Añadir las últimas líneas
                f.write(ultimas_lineas)
            
            # Leer el archivo temporal
            df_temp = pd.read_csv(temp_file, on_bad_lines='skip')
            ultimas_filas = df_temp.tail(5)
            
            # Calcular número de columnas en las últimas filas
            num_columnas_final = ultimas_filas.shape[1]
            print(f"Número de columnas en las últimas filas: {num_columnas_final}")
            
            # Eliminar archivo temporal
            try:
                os.remove(temp_file)
            except:
                pass
        else:
            # Para archivos pequeños, leer directamente con pandas
            df_completo = pd.read_csv(
                archivo_csv,
                low_memory=False,
                on_bad_lines='skip'
            )
            ultimas_filas = df_completo.tail(5)
            
            # Calcular número de columnas en las últimas filas
            num_columnas_final = ultimas_filas.shape[1]
            print(f"Número de columnas en las últimas filas: {num_columnas_final}")
    except Exception as e:
        print(f"Error al obtener las últimas filas: {e}")
        # Si falla, mostrar mensaje en lugar de datos
        ultimas_filas = pd.DataFrame({'Mensaje': ['No se pudieron cargar las últimas filas debido a inconsistencias en el archivo CSV.']})
        num_columnas_final = "No disponible"
    
    # Crear nombre para archivo HTML
    archivo_html = f'tabla_datos_{timestamp}.html'
    
    # Generar HTML con la información de columnas
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>Muestra del CSV</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; }}
            h1 {{ color: #2c3e50; text-align: center; margin-bottom: 20px; }}
            h2 {{ color: #3498db; margin-top: 30px; margin-bottom: 15px; }}
            .container {{ max-width: 95%; margin: 0 auto; }}
            table {{ border-collapse: collapse; width: 100%; margin: 0 auto 30px; }}
            th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
            th {{ background-color: #f2f2f2; }}
            tr:nth-child(even) {{ background-color: #f9f9f9; }}
            tr:hover {{ background-color: #f1f1f1; }}
            .note {{ background-color: #f8f9fa; padding: 10px; border-left: 4px solid #3498db; margin-bottom: 20px; }}
            .stats {{ display: flex; justify-content: space-around; background-color: #e8f4f8; padding: 15px; border-radius: 5px; margin-bottom: 20px; }}
            .stat-box {{ text-align: center; padding: 10px; }}
            .stat-value {{ font-size: 24px; font-weight: bold; color: #2980b9; }}
            .stat-label {{ font-size: 14px; color: #7f8c8d; }}
            .diff {{ color: {('#e74c3c' if num_columnas_inicio != num_columnas_final and num_columnas_final != "No disponible" else '#27ae60')}; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Muestra de {archivo_csv}</h1>
            
            <div class="note">
                <p>Este archivo CSV contiene inconsistencias en su estructura que pueden dificultar su lectura completa.</p>
            </div>
            
            <div class="stats">
                <div class="stat-box">
                    <div class="stat-value">{num_columnas_inicio}</div>
                    <div class="stat-label">Columnas al inicio</div>
                </div>
                <div class="stat-box">
                    <div class="stat-value {('diff' if num_columnas_inicio != num_columnas_final and num_columnas_final != "No disponible" else '')}">{num_columnas_final}</div>
                    <div class="stat-label">Columnas al final</div>
                </div>
                {f'<div class="stat-box"><div class="stat-value diff">{abs(int(num_columnas_inicio) - int(num_columnas_final))}</div><div class="stat-label">Diferencia</div></div>' if isinstance(num_columnas_final, int) and num_columnas_inicio != num_columnas_final else ''}
            </div>
            
            <h2>Primeras 5 filas</h2>
            {primeras_filas.to_html(index=False, classes='dataframe', border=0)}
            
            <h2>Últimas 5 filas (aproximado)</h2>
            {ultimas_filas.to_html(index=False, classes='dataframe', border=0)}
        </div>
    </body>
    </html>
    """
    
    # Guardar y abrir el HTML
    with open(archivo_html, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    ruta_absoluta = os.path.abspath(archivo_html)
    webbrowser.open('file://' + ruta_absoluta, new=2)
    
    print(f"Vista rápida generada en: {ruta_absoluta}")
    
except Exception as e:
    print(f"Error general: {str(e)}")