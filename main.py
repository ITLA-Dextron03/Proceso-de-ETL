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
    try:
        params = urllib.parse.quote_plus(
            f'DRIVER={{{DB_DRIVER}}};'
            f'SERVER={DB_SERVER};'
            f'DATABASE={DB_NAME};'
            f'Trusted_Connection=yes;'
        )
        engine = create_engine(f'mssql+pyodbc:///?odbc_connect={params}')
        with engine.connect() as conn:
            print("✅ Conexión a la base de datos exitosa.")
        return engine
    except Exception as e:
        print(f"❌ Error al conectar con la base de datos: {e}")
        sys.exit()

def limpiar_id(id_str, prefijo):
    if isinstance(id_str, str):
        return pd.to_numeric(id_str.replace(prefijo, ''), errors='coerce')
    return pd.to_numeric(id_str, errors='coerce')

# --- FUNCIONES DE CARGA (USAN CONEXIÓN EXISTENTE) ---
def load_data_conditionally(connection, df, table_name, pk_column):
    if df.empty:
        print(f"No hay nuevos registros para '{table_name}'.")
        return True
    try:
        existing_ids = set(pd.read_sql(f"SELECT {pk_column} FROM {table_name}", connection)[pk_column])
        df_to_load = df[~df[pk_column].isin(existing_ids)]
        if df_to_load.empty:
            print(f"Todos los registros para '{table_name}' ya existen.")
            return True
        df_to_load.to_sql(table_name, connection, if_exists='append', index=False)
        print(f"{len(df_to_load)} nuevos registros cargados en '{table_name}'.")
        return True
    except Exception as e:
        print(f"Error al cargar '{table_name}': {e}")
        return False

def load_dimension_conditionally(connection, df, table_name, name_column):
    if df.empty:
        return True
    try:
        existing_names = set(pd.read_sql(f"SELECT {name_column} FROM {table_name}", connection)[name_column])
        df_to_load = df[~df[name_column].isin(existing_names)]
        if df_to_load.empty:
            return True
        df_to_load.to_sql(table_name, connection, if_exists='append', index=False)
        print(f"{len(df_to_load)} nuevos registros cargados en '{table_name}'.")
        return True
    except Exception as e:
        print(f"Error al cargar '{table_name}': {e}")
        return False

# --- FASES DEL ETL ---

def extract_data(file_paths):
    print("\n--- Fase 1: Extrayendo datos ---")
    try:
        dfs = {name: pd.read_csv(path) for name, path in file_paths.items()}
        print("Datos extraídos.")
        return dfs
    except FileNotFoundError as e:
        print(f"No se encontró el archivo {e.filename}. Abortando.")
        sys.exit()

def prepare_and_load_dimensions(engine, dfs):
    print("\n--- Fase 2: Cargando dimensiones ---")
    with engine.connect() as connection:
        with connection.begin(): # Transacción para dimensiones
            load_dimension_conditionally(connection, pd.DataFrame(dfs['products']['Categoría'].dropna().unique(), columns=['Nombre']), 'Categorias', 'Nombre')
            load_dimension_conditionally(connection, pd.DataFrame(dfs['surveys']['Clasificacion'].dropna().unique(), columns=['Nombre']), 'Clasificaciones', 'Nombre')
            load_dimension_conditionally(connection, pd.DataFrame(dfs['social_comments']['Fuente'].dropna().unique(), columns=['Nombre']), 'Fuentes', 'Nombre')
            cargas_df = dfs['fuente_datos'][['TipoFuente', 'FechaCarga']].copy()
            cargas_df.rename(columns={'TipoFuente': 'Nombre'}, inplace=True)
            cargas_df.drop_duplicates(subset=['Nombre'], keep='first', inplace=True)
            cargas_df['FechaCarga'] = pd.to_datetime(cargas_df['FechaCarga'], errors='coerce')
            cargas_df.dropna(subset=['FechaCarga'], inplace=True)
            load_dimension_conditionally(connection, cargas_df, 'RegistroCargas', 'Nombre')

def get_id_maps(engine):
    print("\n--- Fase 3: Mapeando IDs ---")
    try:
        with engine.connect() as connection:
            return {
                "categoria": pd.read_sql('SELECT IdCategoria, Nombre FROM Categorias', connection, index_col='Nombre').to_dict()['IdCategoria'],
                "clasificacion": pd.read_sql('SELECT IdClasificacion, Nombre FROM Clasificaciones', connection, index_col='Nombre').to_dict()['IdClasificacion'],
                "fuentes": pd.read_sql('SELECT IdFuente, Nombre FROM Fuentes', connection, index_col='Nombre').to_dict()['IdFuente'],
                "cargas": pd.read_sql('SELECT IdCarga, Nombre FROM RegistroCargas', connection, index_col='Nombre').to_dict()['IdCarga']
            }
    except Exception as e:
        print(f"Error al mapear IDs: {e}. Abortando.")
        sys.exit()

def transform_data(dfs, id_maps):
    print("\n--- Fase 4: Transformando datos ---")
    # Clientes
    df_clientes = dfs['clients'].copy()
    df_clientes['IdCliente'] = df_clientes['IdCliente'].astype('Int64')
    df_clientes.drop_duplicates(subset=['IdCliente'], keep='first', inplace=True)
    required_clients = pd.concat([dfs['social_comments']['IdCliente'].apply(limpiar_id, prefijo='C'), dfs['surveys']['IdCliente'], dfs['web_reviews']['IdCliente'].apply(limpiar_id, prefijo='C')]).dropna().unique()
    missing_clients = set(required_clients) - set(df_clientes['IdCliente'])
    if missing_clients:
        placeholders = pd.DataFrame([{'IdCliente': cid, 'Nombre': f'Cliente_{cid}', 'Email': f'cliente_{cid}@mail.com'} for cid in missing_clients])
        df_clientes = pd.concat([df_clientes, placeholders], ignore_index=True)
    email_duplicates = df_clientes.duplicated(subset=['Email'], keep=False)
    df_clientes.loc[email_duplicates, 'Email'] = df_clientes.loc[email_duplicates, 'IdCliente'].apply(lambda cid: f'cliente_{cid}@placeholder.com')

    # Productos
    df_productos = dfs['products'].copy()
    df_productos['IdCategoria'] = df_productos['Categoría'].map(id_maps['categoria'])

    valid_client_ids = set(df_clientes['IdCliente'])

    # Comentarios
    df_comentarios = dfs['social_comments'].copy()
    df_comentarios['IdCliente'] = df_comentarios['IdCliente'].apply(limpiar_id, prefijo='C')
    df_comentarios['IdProducto'] = df_comentarios['IdProducto'].apply(limpiar_id, prefijo='P')
    df_comentarios = df_comentarios[df_comentarios['IdCliente'].isin(valid_client_ids)]
    df_comentarios['IdFuente'] = df_comentarios['Fuente'].map(id_maps['fuentes'])
    df_comentarios.dropna(subset=['IdCliente', 'IdProducto', 'IdFuente'], inplace=True)
    df_comentarios_final = df_comentarios.astype({'IdCliente': 'int', 'IdProducto': 'int', 'IdFuente': 'int'})[['IdComment', 'IdCliente', 'IdProducto', 'IdFuente', 'Fecha', 'comentario']].rename(columns={'comentario': 'Comentario'})

    # Encuestas
    df_encuestas = dfs['surveys'].copy()
    df_encuestas['IdCliente'] = df_encuestas['IdCliente'].astype('Int64')
    df_encuestas = df_encuestas[df_encuestas['IdCliente'].isin(valid_client_ids)]
    df_encuestas['IdClasificacion'] = df_encuestas['Clasificacion'].map(id_maps['clasificacion'])
    df_encuestas['IdCarga'] = id_maps['cargas'].get('CSV')
    df_encuestas_final = df_encuestas.dropna(subset=['IdCliente', 'IdProducto', 'IdClasificacion'])[['IdOpinion', 'IdCliente', 'IdProducto', 'IdCarga', 'Fecha', 'Comentario', 'IdClasificacion', 'PuntajeSatisfaccion']]

    # WebReviews
    df_webreviews = dfs['web_reviews'].copy()
    df_webreviews['IdCliente'] = df_webreviews['IdCliente'].apply(limpiar_id, prefijo='C')
    df_webreviews['IdProducto'] = df_webreviews['IdProducto'].apply(limpiar_id, prefijo='P')
    df_webreviews = df_webreviews[df_webreviews['IdCliente'].isin(valid_client_ids)]
    df_webreviews['IdCarga'] = id_maps['cargas'].get('Web')
    df_webreviews_final = df_webreviews.dropna(subset=['IdCliente', 'IdProducto'])[['IdReview', 'IdCliente', 'IdProducto', 'IdCarga', 'Fecha', 'Comentario', 'Rating']]

    print("✅ Datos transformados.")
    return {
        "clientes": df_clientes,
        "productos": df_productos,
        "comentarios": df_comentarios_final,
        "encuestas": df_encuestas_final,
        "webreviews": df_webreviews_final
    }

def load_main_tables(engine, data_to_load):
    print("\n--- Fase 5: Cargando tablas principales ---")
    with engine.connect() as connection:
        with connection.begin() as transaction:
            try:
                print("Cargando Clientes...")
                if not load_data_conditionally(connection, data_to_load['clientes'], 'Clientes', 'IdCliente'):
                    raise Exception("Fallo en la carga de Clientes")

                print("Cargando Productos...")
                productos_a_cargar = data_to_load['productos'][['IdProducto', 'Nombre', 'IdCategoria']].dropna(subset=['IdProducto'])
                if not load_data_conditionally(connection, productos_a_cargar, 'Productos', 'IdProducto'):
                    raise Exception("Fallo en la carga de Productos")

                print("Cargando Comentarios...")
                if not load_data_conditionally(connection, data_to_load['comentarios'], 'Comentarios', 'IdComment'):
                    raise Exception("Fallo en la carga de Comentarios")

                print("Cargando Encuestas...")
                if not load_data_conditionally(connection, data_to_load['encuestas'], 'Encuestas', 'IdOpinion'):
                    raise Exception("Fallo en la carga de Encuestas")

                print("Cargando WebReviews...")
                if not load_data_conditionally(connection, data_to_load['webreviews'], 'WebReviews', 'IdReview'):
                    raise Exception("Fallo en la carga de WebReviews")
                
                print("Carga de tablas principales completada.")
            except Exception as e:
                print(f"Error durante la carga: {e}. Revirtiendo transacción.")
                transaction.rollback()

def main():
    """Función principal que orquesta el proceso ETL completo."""
    try:
        engine = create_db_engine()
        source_dfs = extract_data(FILE_PATHS)
        prepare_and_load_dimensions(engine, source_dfs)
        id_maps = get_id_maps(engine)
        transformed_data = transform_data(source_dfs, id_maps)
        load_main_tables(engine, transformed_data)
        print("\¡Proceso ETL completado!")
    except Exception as e:
        print(f"Ocurrió un error inesperado en el flujo principal: {e}")

if __name__ == "__main__":
    main()
