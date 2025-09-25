import pandas as pd
from sqlalchemy import create_engine, text
import urllib
import sys
import datetime

# --- CONFIGURACIÓN DE LA BASE DE DATOS ---
DB_SERVER = "DESKTOP-ENGONQR"
DB_NAME = "OpinionDB"
DB_DRIVER = "ODBC Driver 17 for SQL Server"

file_paths = {
    "clients": "./csv/clients.csv",
    "products": "./csv/products.csv",
    "fuente_datos": "./csv/fuente_datos.csv",
    "social_comments": "./csv/social_comments.csv",
    "surveys": "./csv/surveys_part1.csv",
    "web_reviews": "./csv/web_reviews.csv"
}

def create_db_engine():
    """Crea y retorna un motor de conexión de SQLAlchemy para SQL Server."""
    try:
        params = urllib.parse.quote_plus(
            f'DRIVER={{{DB_DRIVER}}};'
            f'SERVER={DB_SERVER};'
            f'DATABASE={DB_NAME};'
            f'Trusted_Connection=yes;'
        )
        engine = create_engine(f'mssql+pyodbc:///?odbc_connect={params}')
        engine.connect()
        print("✅ Conexión a la base de datos exitosa.")
        return engine
    except Exception as e:
        print(f"Error al conectar con la base de datos: {e}")
        sys.exit()

def limpiar_id(id_str, prefijo):
    """Limpia un ID eliminando un prefijo y convirtiéndolo a entero."""
    if isinstance(id_str, str) and id_str.startswith(prefijo):
        return pd.to_numeric(id_str.replace(prefijo, ''), errors='coerce')
    return pd.to_numeric(id_str, errors='coerce')

def main():
    """Función principal que ejecuta el proceso ETL completo."""
    engine = create_db_engine()

    # --- FASE 1: EXTRACCIÓN ---
    print("\n--- Fase 1: Extrayendo datos de los archivos CSV ---")
    try:
        dfs = {name: pd.read_csv(path) for name, path in file_paths.items()}
        print("Todos los archivos CSV han sido cargados correctamente.")
    except FileNotFoundError as e:
        print(f"Error: No se encontró el archivo {e.filename}.")
        return

    # --- FASE 2: PREPARACIÓN Y CARGA DE DIMENSIONES ---
    print("\n--- Fase 2: Preparando y cargando tablas de dimensión ---")
    
    try:
        # 1. Poblar tablas de dimensión simples
        categorias_df = pd.DataFrame(dfs['products']['Categoría'].dropna().unique(), columns=['Nombre'])
        clasificaciones_df = pd.DataFrame(dfs['surveys']['Clasificacion'].unique(), columns=['Nombre'])
        
        categorias_df.to_sql('Categorias', engine, if_exists='append', index=False)
        clasificaciones_df.to_sql('Clasificaciones', engine, if_exists='append', index=False)
        print("- Dimensiones 'Categorias' y 'Clasificaciones' cargadas.")

        # 2. Poblar la tabla de orígenes de comentarios (Fuentes)
        fuentes_sociales_df = pd.DataFrame(dfs['social_comments']['Fuente'].dropna().unique(), columns=['Nombre'])
        fuentes_sociales_df.to_sql('Fuentes', engine, if_exists='append', index=False)
        print("- Dimensión 'Fuentes' (redes sociales) cargada.")

        # 3. Poblar la bitácora de cargas (RegistroCargas)
        cargas_df = dfs['fuente_datos'][['TipoFuente', 'FechaCarga']].copy()
        cargas_df.rename(columns={'TipoFuente': 'Nombre'}, inplace=True)
        cargas_df.drop_duplicates(subset=['Nombre'], keep='first', inplace=True)
        
        cargas_df.to_sql('RegistroCargas', engine, if_exists='append', index=False)
        print("- Dimensión 'RegistroCargas' (bitácora) cargada con valores únicos.")
        
        # --- FASE 3: MAPEO DE IDS ---
        print("\n--- Fase 3: Mapeando IDs desde la base de datos ---")

        map_categoria = pd.read_sql('SELECT IdCategoria, Nombre FROM Categorias', engine, index_col='Nombre').to_dict()['IdCategoria']
        map_clasificacion = pd.read_sql('SELECT IdClasificacion, Nombre FROM Clasificaciones', engine, index_col='Nombre').to_dict()['IdClasificacion']
        map_fuentes = pd.read_sql('SELECT IdFuente, Nombre FROM Fuentes', engine, index_col='Nombre').to_dict()['IdFuente']
        map_cargas = pd.read_sql('SELECT IdCarga, Nombre FROM RegistroCargas', engine, index_col='Nombre').to_dict()['IdCarga']
        
        print("Mapeo de todos los IDs completado.")

        # --- FASE 4: PREPARACIÓN Y CARGA DE DATOS PRINCIPALES ---
        print("\n--- Fase 4: Preparando y cargando datos principales ---")
        
        # ✨ INICIO DE LÓGICA DE CONSISTENCIA DE DATOS (PLACEHOLDERS) ✨
        print("Verificando consistencia de datos de Clientes y Productos...")
        df_clientes_final = dfs['clients'].copy()
        df_productos_final = dfs['products'].copy()

        # Consistencia para Clientes
        existing_client_ids = set(df_clientes_final['IdCliente'])
        survey_client_ids = set(dfs['surveys']['IdCliente'])
        missing_client_ids = survey_client_ids - existing_client_ids
        if missing_client_ids:
            print(f"Se encontraron {len(missing_client_ids)} clientes faltantes. Creando placeholders...")
            missing_clients_df = pd.DataFrame([
                {'IdCliente': cid, 'Nombre': f'Cliente_{cid}', 'Email': f'cliente{cid}@mail.com'}
                for cid in missing_client_ids
            ])
            df_clientes_final = pd.concat([df_clientes_final, missing_clients_df], ignore_index=True)
            print("Placeholders de Clientes añadidos.")

        # Consistencia para Productos
        existing_product_ids = set(df_productos_final['IdProducto'])
        product_ids_surveys = set(dfs['surveys']['IdProducto'])
        product_ids_comments = set(dfs['social_comments']['IdProducto'].apply(lambda x: limpiar_id(x, 'P')).dropna())
        product_ids_reviews = set(dfs['web_reviews']['IdProducto'].apply(lambda x: limpiar_id(x, 'P')).dropna())
        all_transactional_product_ids = product_ids_surveys.union(product_ids_comments).union(product_ids_reviews)
        missing_product_ids = all_transactional_product_ids - existing_product_ids
        if missing_product_ids:
            print(f"Se encontraron {len(missing_product_ids)} productos faltantes. Creando placeholders...")
            missing_products_df = pd.DataFrame([
                {'IdProducto': pid, 'Nombre': f'Producto_{pid}', 'Categoría': None}
                for pid in missing_product_ids
            ])
            df_productos_final = pd.concat([df_productos_final, missing_products_df], ignore_index=True)
            print("Placeholders de Productos añadidos.")
        # ✨ FIN DE LÓGICA DE CONSISTENCIA ✨

        # Preparar Productos con su IdCategoria
        df_productos_final['IdCategoria'] = df_productos_final['Categoría'].map(map_categoria)
        df_productos_final = df_productos_final[['IdProducto', 'Nombre', 'IdCategoria']]
        
        # Preparar Comentarios
        df_comentarios = dfs['social_comments'].copy()
        df_comentarios.dropna(subset=['IdCliente', 'IdProducto', 'Fuente'], inplace=True)
        df_comentarios['IdCliente'] = df_comentarios['IdCliente'].apply(lambda x: limpiar_id(x, 'C')).astype(int)
        df_comentarios['IdProducto'] = df_comentarios['IdProducto'].apply(lambda x: limpiar_id(x, 'P')).astype(int)
        df_comentarios['IdFuente'] = df_comentarios['Fuente'].map(map_fuentes)
        df_comentarios_final = df_comentarios[['IdComment', 'IdCliente', 'IdProducto', 'IdFuente', 'Fecha', 'comentario']].rename(columns={'comentario': 'Comentario'})

        # Preparar Encuestas y WebReviews
        id_carga_csv = map_cargas.get('CSV')
        id_carga_web = map_cargas.get('Web')

        df_encuestas = dfs['surveys'].copy()
        df_encuestas['IdClasificacion'] = df_encuestas['Clasificacion'].map(map_clasificacion)
        df_encuestas['IdCarga'] = id_carga_csv
        df_encuestas_final = df_encuestas[['IdOpinion', 'IdCliente', 'IdProducto', 'IdCarga', 'Fecha', 'Comentario', 'IdClasificacion', 'PuntajeSatisfaccion']]
        
        df_webreviews = dfs['web_reviews'].copy()
        df_webreviews.dropna(subset=['IdCliente', 'IdProducto'], inplace=True)
        df_webreviews['IdCliente'] = df_webreviews['IdCliente'].apply(lambda x: limpiar_id(x, 'C')).astype(int)
        df_webreviews['IdProducto'] = df_webreviews['IdProducto'].apply(lambda x: limpiar_id(x, 'P')).astype(int)
        df_webreviews['IdCarga'] = id_carga_web
        df_webreviews_final = df_webreviews[['IdReview', 'IdCliente', 'IdProducto', 'IdCarga', 'Fecha', 'Comentario', 'Rating']]

        print("Todos los datos han sido transformados y están listos para cargar.")

        # Cargar tablas principales
        print("Cargando tablas principales en el orden correcto...")
        with engine.begin() as connection:
            df_clientes_final.to_sql('Clientes', connection, if_exists='append', index=False)
            print(f"- Clientes cargado ({len(df_clientes_final)} registros).")
            
            df_productos_final.to_sql('Productos', connection, if_exists='append', index=False)
            print(f"- Productos cargado ({len(df_productos_final)} registros).")

        # Cargar tablas de hechos
        df_comentarios_final.to_sql('Comentarios', engine, if_exists='append', index=False)
        print(f"- Comentarios cargado ({len(df_comentarios_final)} registros).")

        df_encuestas_final.to_sql('Encuestas', engine, if_exists='append', index=False)
        print(f"- Encuestas cargado ({len(df_encuestas_final)} registros).")

        df_webreviews_final.to_sql('WebReviews', engine, if_exists='append', index=False)
        print(f"- WebReviews cargado ({len(df_webreviews_final)} registros).")
        
        print("\n¡Proceso ETL completado con éxito con la nueva estructura!")

    except Exception as e:
        print(f"Ocurrió un error durante el proceso: {e}")
        print("Asegúrate de haber ejecutado el último script SQL sobre una base de datos limpia.")

if __name__ == "__main__":
    main()