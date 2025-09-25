import pandas as pd
from sqlalchemy import create_engine
import urllib
import sys

# --- CONFIGURACIÓN ---
DB_SERVER = "DESKTOP-ENGONQR"
DB_NAME = "OpinionDB"
DB_DRIVER = "ODBC Driver 17 for SQL Server"

FILE_PATHS = {
    "clients": "./csv/clients.csv",
    "products": "./csv/products.csv",
    "fuente_datos": "./csv/fuente_datos.csv",
    "social_comments": "./csv/social_comments.csv",
    "surveys": "./csv/surveys_part1.csv",
    "web_reviews": "./csv/web_reviews.csv"
}

# --- FUNCIONES DE UTILIDAD ---

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
        with engine.connect() as connection:
            print("Conexión a la base de datos exitosa.")
        return engine
    except Exception as e:
        print(f"Error al conectar con la base de datos: {e}")
        sys.exit()

def limpiar_id(id_str, prefijo):
    """Limpia un ID eliminando un prefijo y convirtiéndolo a entero."""
    if isinstance(id_str, str) and id_str.startswith(prefijo):
        return pd.to_numeric(id_str.replace(prefijo, ''), errors='coerce')
    return pd.to_numeric(id_str, errors='coerce')

def load_data_conditionally(df, table_name, pk_column, engine):
    """Carga un DataFrame en una tabla, evitando duplicados basados en una clave primaria."""
    if df.empty:
        print(f"No hay nuevos registros para cargar en '{table_name}'.")
        return
    
    try:
        with engine.connect() as connection:
            existing_ids = set(pd.read_sql(f"SELECT {pk_column} FROM {table_name}", connection)[pk_column])
            df_to_load = df[~df[pk_column].isin(existing_ids)]
            
            if df_to_load.empty:
                print(f"Todos los registros para '{table_name}' ya existen en la base de datos.")
                return

            df_to_load.to_sql(table_name, connection, if_exists='append', index=False)
            print(f"✅ {len(df_to_load)} nuevos registros cargados en '{table_name}'.")
    except Exception as e:
        print(f"Error al cargar datos en '{table_name}': {e}")

def load_dimension_conditionally(df, table_name, name_column, engine):
    """Carga un DataFrame de dimensión, evitando duplicados basados en la columna de nombre."""
    if df.empty:
        print(f"No hay datos para la dimensión '{table_name}'.")
        return
        
    try:
        with engine.connect() as connection:
            existing_names = set(pd.read_sql(f"SELECT {name_column} FROM {table_name}", connection)[name_column])
            df_to_load = df[~df[name_column].isin(existing_names)]
            
            if df_to_load.empty:
                print(f"Todos los registros para la dimensión '{table_name}' ya existen.")
                return
            
            df_to_load.to_sql(table_name, connection, if_exists='append', index=False)
            print(f"✅ {len(df_to_load)} nuevos registros cargados en la dimensión '{table_name}'.")
    except Exception as e:
        print(f"Error al cargar la dimensión '{table_name}': {e}")

# --- FASES DEL ETL ---

def extract_data(file_paths):
    """Fase 1: Extrae datos de los archivos CSV a DataFrames de pandas."""
    print("\n--- Fase 1: Extrayendo datos de los archivos CSV ---")
    try:
        dfs = {name: pd.read_csv(path) for name, path in file_paths.items()}
        print("✅ Todos los archivos CSV han sido cargados correctamente.")
        return dfs
    except FileNotFoundError as e:
        print(f"❌ Error: No se encontró el archivo {e.filename}. Abortando proceso.")
        sys.exit()

def prepare_and_load_dimensions(engine, dfs):
    """Fase 2: Prepara y carga las tablas de dimensión a partir de los DataFrames."""
    print("\n--- Fase 2: Preparando y cargando tablas de dimensión ---")
    
   
    categorias_df = pd.DataFrame(dfs['products']['Categoría'].dropna().unique(), columns=['Nombre'])
    load_dimension_conditionally(categorias_df, 'Categorias', 'Nombre', engine)
   
    clasificaciones_df = pd.DataFrame(dfs['surveys']['Clasificacion'].dropna().unique(), columns=['Nombre'])
    load_dimension_conditionally(clasificaciones_df, 'Clasificaciones', 'Nombre', engine)

    fuentes_df = pd.DataFrame(dfs['social_comments']['Fuente'].dropna().unique(), columns=['Nombre'])
    load_dimension_conditionally(fuentes_df, 'Fuentes', 'Nombre', engine)

    cargas_df = dfs['fuente_datos'][['TipoFuente', 'FechaCarga']].copy()
    cargas_df.rename(columns={'TipoFuente': 'Nombre'}, inplace=True)
    cargas_df.drop_duplicates(subset=['Nombre'], keep='first', inplace=True)
    load_dimension_conditionally(cargas_df, 'RegistroCargas', 'Nombre', engine)

def get_id_maps(engine):
    """Fase 3: Crea diccionarios de mapeo de IDs desde la base de datos."""
    print("\n--- Fase 3: Mapeando IDs desde la base de datos ---")
    try:
        with engine.connect() as connection:
            map_categoria = pd.read_sql('SELECT IdCategoria, Nombre FROM Categorias', connection, index_col='Nombre').to_dict()['IdCategoria']
            map_clasificacion = pd.read_sql('SELECT IdClasificacion, Nombre FROM Clasificaciones', connection, index_col='Nombre').to_dict()['IdClasificacion']
            map_fuentes = pd.read_sql('SELECT IdFuente, Nombre FROM Fuentes', connection, index_col='Nombre').to_dict()['IdFuente']
            map_cargas = pd.read_sql('SELECT IdCarga, Nombre FROM RegistroCargas', connection, index_col='Nombre').to_dict()['IdCarga']
        print("Mapeo de todos los IDs completado.")
        return {
            "categoria": map_categoria,
            "clasificacion": map_clasificacion,
            "fuentes": map_fuentes,
            "cargas": map_cargas
        }
    except Exception as e:
        print(f"Error al crear los mapas de IDs: {e}. Abortando proceso.")
        sys.exit()

def transform_and_ensure_consistency(dfs, id_maps):
    """Fase 4: Transforma los datos principales y asegura la consistencia referencial."""
    print("\n--- Fase 4: Transformando datos y asegurando consistencia ---")

    df_clientes = dfs['clients'].copy()
    df_productos = dfs['products'].copy()

    existing_client_ids = set(df_clientes['IdCliente'])
    survey_client_ids = set(dfs['surveys']['IdCliente'])
    missing_client_ids = survey_client_ids - existing_client_ids
    if missing_client_ids:
        print(f"Se encontraron {len(missing_client_ids)} clientes faltantes. Creando placeholders...")
        missing_clients_df = pd.DataFrame([
            {'IdCliente': cid, 'Nombre': f'Cliente_{cid}', 'Email': f'cliente{cid}@mail.com'}
            for cid in missing_client_ids
        ])
        df_clientes = pd.concat([df_clientes, missing_clients_df], ignore_index=True)

    existing_product_ids = set(df_productos['IdProducto'])
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
        df_productos = pd.concat([df_productos, missing_products_df], ignore_index=True)

    df_productos['IdCategoria'] = df_productos['Categoría'].map(id_maps['categoria'])
    df_productos_final = df_productos[['IdProducto', 'Nombre', 'IdCategoria']]

    df_comentarios = dfs['social_comments'].copy()
    df_comentarios.dropna(subset=['IdCliente', 'IdProducto', 'Fuente'], inplace=True)
    df_comentarios['IdCliente'] = df_comentarios['IdCliente'].apply(lambda x: limpiar_id(x, 'C')).astype(int)
    df_comentarios['IdProducto'] = df_comentarios['IdProducto'].apply(lambda x: limpiar_id(x, 'P')).astype(int)
    df_comentarios['IdFuente'] = df_comentarios['Fuente'].map(id_maps['fuentes'])
    df_comentarios_final = df_comentarios[['IdComment', 'IdCliente', 'IdProducto', 'IdFuente', 'Fecha', 'comentario']].rename(columns={'comentario': 'Comentario'})

    df_encuestas = dfs['surveys'].copy()
    df_encuestas['IdClasificacion'] = df_encuestas['Clasificacion'].map(id_maps['clasificacion'])
    df_encuestas['IdCarga'] = id_maps['cargas'].get('CSV')
    df_encuestas_final = df_encuestas[['IdOpinion', 'IdCliente', 'IdProducto', 'IdCarga', 'Fecha', 'Comentario', 'IdClasificacion', 'PuntajeSatisfaccion']]
    
    df_webreviews = dfs['web_reviews'].copy()
    df_webreviews.dropna(subset=['IdCliente', 'IdProducto'], inplace=True)
    df_webreviews['IdCliente'] = df_webreviews['IdCliente'].apply(lambda x: limpiar_id(x, 'C')).astype(int)
    df_webreviews['IdProducto'] = df_webreviews['IdProducto'].apply(lambda x: limpiar_id(x, 'P')).astype(int)
    df_webreviews['IdCarga'] = id_maps['cargas'].get('Web')
    df_webreviews_final = df_webreviews[['IdReview', 'IdCliente', 'IdProducto', 'IdCarga', 'Fecha', 'Comentario', 'Rating']]

    print("Todos los datos han sido transformados.")
    
    return {
        "clientes": df_clientes,
        "productos": df_productos_final,
        "comentarios": df_comentarios_final,
        "encuestas": df_encuestas_final,
        "webreviews": df_webreviews_final
    }

def load_main_tables(engine, data_to_load):
    """Fase 5: Carga los datos principales y de hechos en la base de datos."""
    print("\n--- Fase 5: Cargando datos principales y de hechos ---")
    
    # El orden es importante por las claves foráneas
    load_data_conditionally(data_to_load['clientes'], 'Clientes', 'IdCliente', engine)
    load_data_conditionally(data_to_load['productos'], 'Productos', 'IdProducto', engine)
    load_data_conditionally(data_to_load['comentarios'], 'Comentarios', 'IdComment', engine)
    load_data_conditionally(data_to_load['encuestas'], 'Encuestas', 'IdOpinion', engine)
    load_data_conditionally(data_to_load['webreviews'], 'WebReviews', 'IdReview', engine)

def main():
    """Función principal que orquesta el proceso ETL completo."""
    try:
        engine = create_db_engine()
        
        # Fases del ETL
        source_dfs = extract_data(FILE_PATHS)
        prepare_and_load_dimensions(engine, source_dfs)
        id_maps = get_id_maps(engine)
        transformed_data = transform_and_ensure_consistency(source_dfs, id_maps)
        load_main_tables(engine, transformed_data)
        
        print("\n¡Proceso ETL completado con éxito!")

    except Exception as e:
        print(f"Ocurrió un error inesperado en el proceso principal: {e}")
        print("Asegúrate de haber ejecutado el script SQL de creación de tablas sobre una base de datos limpia.")

if __name__ == "__main__":
    main()
