import pandas as pd
import os
import webbrowser
from datetime import datetime

# ==========================================
# CONFIGURACIÓN - MODIFICA ESTOS PARÁMETROS
# ==========================================

# Ruta del archivo CSV que quieres visualizar
archivo_csv = 'visitas_expandidas.csv'  # Cambia esto a la ruta de tu archivo

# Nombre del archivo HTML de salida (se creará en el directorio actual)
archivo_html = 'tabla_datos.html'

# Número de filas a mostrar (None para todas)
n_filas = 10

# Columnas específicas a mostrar (None para todas)
columnas = None  # Ejemplo: ['columna1', 'columna2']

# Abrir automáticamente en el navegador
abrir_en_navegador = True

# ==========================================
# FIN DE LA CONFIGURACIÓN
# ==========================================

def visualizar_csv_como_tabla_html(archivo_csv, archivo_html, n_filas=None, columnas=None, abrir_navegador=True):
    """
    Visualiza un archivo CSV como una tabla HTML y la guarda en un archivo
    
    Parámetros:
    - archivo_csv: ruta al archivo CSV
    - archivo_html: nombre del archivo HTML de salida
    - n_filas: número de filas a mostrar (None para todas)
    - columnas: lista de columnas específicas a mostrar (None para todas)
    - abrir_navegador: si es True, abre el archivo HTML en el navegador predeterminado
    
    Retorna:
    - Ruta al archivo HTML generado
    """
    try:
        # Verificar si el archivo existe
        if not os.path.exists(archivo_csv):
            print(f"⚠️ El archivo {archivo_csv} no existe.")
            return None
            
        # Información del archivo
        tamano_mb = os.path.getsize(archivo_csv) / (1024 * 1024)
        print(f"📊 Archivo CSV: {archivo_csv} ({tamano_mb:.2f} MB)")
        
        # Leer el archivo CSV
        if n_filas is not None:
            df = pd.read_csv(archivo_csv, nrows=n_filas)
            print(f"Leyendo las primeras {n_filas} filas...")
        else:
            df = pd.read_csv(archivo_csv)
            print(f"Leyendo todas las filas ({len(df)})...")
        
        # Seleccionar columnas específicas si se indicaron
        if columnas is not None:
            # Verificar que las columnas existen
            cols_existentes = [col for col in columnas if col in df.columns]
            cols_faltantes = [col for col in columnas if col not in df.columns]
            
            if cols_faltantes:
                print(f"⚠️ Algunas columnas no existen en el archivo: {cols_faltantes}")
            
            if cols_existentes:
                df = df[cols_existentes]
            else:
                print("❌ Ninguna de las columnas especificadas existe en el archivo.")
                print(f"Columnas disponibles: {df.columns.tolist()}")
                return None
        
        # Información básica del DataFrame
        print(f"📋 Dimensiones: {df.shape[0]} filas × {df.shape[1]} columnas")
        
        # Crear HTML con estilos CSS
        html_string = f'''
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <title>Visualización de CSV: {os.path.basename(archivo_csv)}</title>
            <style>
                body {{
                    font-family: Arial, sans-serif;
                    margin: 20px;
                    background-color: #f5f5f5;
                }}
                .container {{
                    background-color: white;
                    padding: 20px;
                    border-radius: 5px;
                    box-shadow: 0 2px 5px rgba(0, 0, 0, 0.1);
                }}
                h1 {{
                    color: #333;
                    border-bottom: 2px solid #4CAF50;
                    padding-bottom: 10px;
                }}
                .info {{
                    background-color: #e7f3fe;
                    border-left: 6px solid #2196F3;
                    padding: 10px;
                    margin: 15px 0;
                }}
                table {{
                    border-collapse: collapse;
                    width: 100%;
                    margin-top: 20px;
                }}
                th, td {{
                    border: 1px solid #ddd;
                    padding: 8px;
                    text-align: left;
                }}
                th {{
                    background-color: #4CAF50;
                    color: white;
                }}
                tr:nth-child(even) {{
                    background-color: #f2f2f2;
                }}
                tr:hover {{
                    background-color: #ddd;
                }}
                .footer {{
                    margin-top: 20px;
                    font-size: 12px;
                    color: #666;
                    text-align: center;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>Visualización de CSV: {os.path.basename(archivo_csv)}</h1>
                <div class="info">
                    <p><strong>Archivo:</strong> {archivo_csv}</p>
                    <p><strong>Tamaño:</strong> {tamano_mb:.2f} MB</p>
                    <p><strong>Filas mostradas:</strong> {len(df)} de {n_filas if n_filas is not None else "todas"}</p>
                    <p><strong>Total columnas:</strong> {df.shape[1]}</p>
                    <p><strong>Generado:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                </div>
                
                {df.to_html(classes='table table-striped', border=0)}
                
                <div class="footer">
                    <p>Generado con Pandas {pd.__version__}</p>
                </div>
            </div>
        </body>
        </html>
        '''
        
        # Guardar el HTML
        with open(archivo_html, 'w', encoding='utf-8') as f:
            f.write(html_string)
        
        # Obtener la ruta absoluta al archivo HTML
        ruta_html_abs = os.path.abspath(archivo_html)
        
        print(f"\n✅ Tabla HTML generada exitosamente:")
        print(f"📄 {ruta_html_abs}")
        
        # Abrir el archivo HTML en el navegador predeterminado
        if abrir_navegador:
            # Convertir a URI para compatibilidad con todos los sistemas operativos
            uri = f"file://{ruta_html_abs}"
            print(f"🌐 Abriendo en el navegador: {uri}")
            webbrowser.open(uri)
        
        return ruta_html_abs
        
    except Exception as e:
        print(f"❌ Error al procesar el archivo: {e}")
        return None

# Ejecutar la función
ruta_html = visualizar_csv_como_tabla_html(
    archivo_csv=archivo_csv,
    archivo_html=archivo_html,
    n_filas=n_filas,
    columnas=columnas,
    abrir_navegador=abrir_en_navegador
)

print("\n" + "="*50)
print("INFORMACIÓN ADICIONAL")
print("="*50)
print(f"✅ Para ver la tabla nuevamente, abre este archivo en tu navegador:")
print(f"📄 {ruta_html}")
print("\n✅ Para abrir programáticamente en cualquier momento:")
print(f"import webbrowser")
print(f"webbrowser.open('file://{os.path.abspath(archivo_html)}')")