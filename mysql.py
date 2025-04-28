import requests
import json
import os
import time
import threading
import mysql.connector
from mysql.connector import Error

# --- Configuration ---
KSQLDB_URL = "http://localhost:8088"
MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_DB = "mydb"
MYSQL_USER = "USER" # A remplacer
MYSQL_PASSWORD = "PASSWORD" # A remplacer

TABLES_TO_SYNC = {
    "USER_TRANSACTION_TOTALS": {
        "mysql_table": "dwh_user_transaction_totals",
        "ksql_pk_cols": ["USER_ID", "TRANSACTION_TYPE"], # Clés DANS le JSON ksqlDB
        "mysql_pk_cols": ["user_id", "transaction_type"],   # Clés DANS MySQL
        "column_mapping": {
            "TOTAL_SPENT": "total_spent" # Mapper KSQL_COL: MYSQL_COL
        }
    },
    "TRANSACTIONS_PURCHASE": {
        "mysql_table": "dwh_windowed_transaction_totals",
        "ksql_pk_cols": ["TRANSACTION_TYPE", "WINDOWSTART"], # Clés DANS le JSON ksqlDB
        "mysql_pk_cols": ["transaction_type", "window_start"],   # Clés DANS MySQL
        "column_mapping": {
            "WINDOWEND": "window_end",
            "TOTAL_DEPENSE": "total_depense"
        }
    },
    "TRANSACTIONS_REFUND": { # Cible la même table MySQL
        "mysql_table": "dwh_windowed_transaction_totals",
        "ksql_pk_cols": ["TRANSACTION_TYPE", "WINDOWSTART"],
        "mysql_pk_cols": ["transaction_type", "window_start"],
        "column_mapping": {
            "WINDOWEND": "window_end",
            "TOTAL_DEPENSE": "total_depense"
        }
    },
    "TRANSACTIONS_PAYMENT": { # Cible la même table MySQL
        "mysql_table": "dwh_windowed_transaction_totals",
        "ksql_pk_cols": ["TRANSACTION_TYPE", "WINDOWSTART"],
        "mysql_pk_cols": ["transaction_type", "window_start"],
        "column_mapping": {
            "WINDOWEND": "window_end",
            "TOTAL_DEPENSE": "total_depense"
        }
    },
    "TRANSACTIONS_WITHDRAWAL": { # Cible la même table MySQL
        "mysql_table": "dwh_windowed_transaction_totals",
        "ksql_pk_cols": ["TRANSACTION_TYPE", "WINDOWSTART"],
        "mysql_pk_cols": ["transaction_type", "window_start"],
        "column_mapping": {
            "WINDOWEND": "window_end",
            "TOTAL_DEPENSE": "total_depense"
        }
    }
}

# --- Fonctions Utilitaires ---

def get_mysql_connection():
    conn = None
    try:
        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            database=MYSQL_DB,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            # Important pour envoyer des Decimals correctement
            raw=False, use_pure=False
        )
    except Error as e:
        print(f"ERROR: MySQL connection failed: {e}")
    return conn

# Fonction UPSERT/DELETE adaptée aux clés composites et au mapping
def execute_mysql_upsert_or_delete(connection, config, primary_key_values, data_map):
    if connection is None or not connection.is_connected():
        print(f"ERROR [Table: {config['mysql_table']}]: No active MySQL connection.")
        return False

    mysql_table = config['mysql_table']
    mysql_pk_cols = config['mysql_pk_cols']
    cursor = None
    success = False

    try:
        cursor = connection.cursor()

        # Construire la clause WHERE pour les PKs composites
        where_clause = " AND ".join([f"{col} = %s" for col in mysql_pk_cols])

        if data_map is None:
            # --- DELETE Operation ---
            sql = f"DELETE FROM {mysql_table} WHERE {where_clause}"
            # print(f"DEBUG DELETE SQL: {sql} PARAMS: {primary_key_values}") # Debug
            cursor.execute(sql, primary_key_values)
            print(f"DEBUG [Table: {mysql_table}]: Executed DELETE for PK {primary_key_values}. Rows affected: {cursor.rowcount}")

        else:
            # --- UPSERT Operation ---
            # Combiner clés primaires et autres colonnes pour l'INSERT
            all_mysql_cols = mysql_pk_cols + list(data_map.keys())
            all_values_placeholders = ["%s"] * len(all_mysql_cols)

            # Construire la partie UPDATE (ne mettre à jour que les colonnes non-PK)
            update_assignments = ", ".join([f"{col} = VALUES({col})" for col in data_map.keys()])

            sql = f"""
                INSERT INTO {mysql_table} ({', '.join(all_mysql_cols)})
                VALUES ({', '.join(all_values_placeholders)})
                ON DUPLICATE KEY UPDATE {update_assignments}
            """

            # Préparer le tuple de valeurs dans le bon ordre (PKs d'abord, puis autres valeurs)
            values_tuple = primary_key_values + tuple(data_map.values())
            cursor.execute(sql, values_tuple)

        connection.commit()
        success = True

    except Error as e:
        print(f"ERROR [Table: {mysql_table}]: MySQL execution failed for PK {primary_key_values}: {e}")
        if connection and connection.is_connected():
             try: connection.rollback(); print("INFO: MySQL transaction rolled back.")
             except Error as rb_err: print(f"ERROR: Rollback failed: {rb_err}")
    except Exception as ex:
         print(f"ERROR [Table: {mysql_table}]: Unexpected error during DB operation for PK {primary_key_values}: {ex}")
         if connection and connection.is_connected(): connection.rollback()

    finally:
        if cursor:
            cursor.close()
    return success


# Fonction principale pour un thread de synchronisation (adaptée)
def sync_table_to_mysql(ksql_url, ksql_table_name, config):
    """Lit les changements d'une table ksqlDB et les applique à MySQL."""
    mysql_table_name = config['mysql_table']
    ksql_pk_cols = config['ksql_pk_cols']
    mysql_pk_cols = config['mysql_pk_cols']
    column_mapping = config['column_mapping']
    sync_key = f"{ksql_table_name} -> {mysql_table_name}" # Pour les logs

    print(f"INFO [Sync: {sync_key}]: Starting sync process...")

    db_conn = get_mysql_connection()
    if not db_conn:
        print(f"ERROR [Sync: {sync_key}]: Could not establish initial MySQL connection. Thread exiting.")
        return

    query = f"SELECT * FROM {ksql_table_name} EMIT CHANGES;"
    url = f"{ksql_url}/query-stream"
    headers = {"Content-Type": "application/vnd.ksql.v1+json"}
    payload = json.dumps({
        "sql": query,
        "properties": {"ksql.streams.auto.offset.reset": "earliest"}
    })

    session = requests.Session()
    last_error_time = 0
    error_delay = 5

    while True:
        try:
            if not db_conn or not db_conn.is_connected():
                print(f"WARN [Sync: {sync_key}]: MySQL connection lost. Attempting reconnect...")
                time.sleep(error_delay) # Attendre avant de retenter
                db_conn = get_mysql_connection()
                if not db_conn:
                    print(f"ERROR [Sync: {sync_key}]: Reconnect failed. Retrying later.")
                    time.sleep(error_delay * 2)
                    continue

            with session.post(url, headers=headers, data=payload, stream=True, timeout=(10, 120)) as response:
                response.raise_for_status()
                print(f"INFO [Sync: {sync_key}]: Connected to ksqlDB stream. Reading changes...")
                last_error_time = 0
                header_processed = False # Flag simple pour ignorer la première ligne d'en-tête

                for line in response.iter_lines():
                    if not line: continue
                    try:
                        decoded_line = line.decode('utf-8').strip()
                        if not decoded_line: continue
                        # print(f"DEBUG Raw Line [{sync_key}]: '{decoded_line}'") # Debug

                        parsed_data = json.loads(decoded_line)
                        record_data = None

                        if not header_processed and isinstance(parsed_data, dict) and "queryId" in parsed_data and "columnNames" in parsed_data:
                             print(f"INFO [Sync: {sync_key}]: Received header - Columns: {parsed_data['columnNames']}")
                             header_processed = True
                             # Validation (optionnelle): Vérifier si toutes les ksql_pk_cols et les clés de column_mapping sont présentes
                             expected_ksql_cols = set(ksql_pk_cols) | set(column_mapping.keys())
                             missing = expected_ksql_cols - set(map(str.upper, parsed_data['columnNames']))
                             if missing:
                                 print(f"WARN [Sync: {sync_key}]: Missing expected columns in ksqlDB output: {missing}")
                             continue

                        # Les données sont généralement dans un dict simple (basé sur vos screenshots p10 et p17)
                        # même pour les clés composites avec KEY_FORMAT='JSON'
                        elif isinstance(parsed_data, dict):
                             record_data = parsed_data
                        # Gérer le cas où ksql renvoie [ {...} ] (moins probable pour les tables mais possible)
                        elif isinstance(parsed_data, list) and len(parsed_data) == 1 and isinstance(parsed_data[0], dict):
                             record_data = parsed_data[0]

                        # Traitement de l'enregistrement de données
                        if record_data:
                            # Extraire les valeurs de la clé primaire ksqlDB
                            pk_values_list = []
                            valid_pk = True
                            for pk_col_ksql in ksql_pk_cols:
                                if pk_col_ksql not in record_data:
                                    print(f"ERROR [Sync: {sync_key}]: PK column '{pk_col_ksql}' not found in record: {record_data}")
                                    valid_pk = False
                                    break
                                pk_values_list.append(record_data[pk_col_ksql])
                            if not valid_pk: continue
                            primary_key_values_tuple = tuple(pk_values_list) # Tuple pour la requête SQL

                            # Construire le dictionnaire de données pour MySQL (mapping + conversion type)
                            data_to_upsert = {}
                            valid_data = True
                            for ksql_col, mysql_col in column_mapping.items():
                                if ksql_col not in record_data:
                                    continue # Ou ignorer cette colonne pour l'instant

                                ksql_value = record_data[ksql_col]

                                # Conversion de type (Exemple pour DECIMAL)
                                if mysql_col in ("total_spent", "total_depense"):
                                    try:
                                        # Gérer le cas où ksqlDB renvoie null
                                        if ksql_value is None:
                                            data_to_upsert[mysql_col] = None
                                        else:
                                            # Convertir en Decimal pour la précision
                                            data_to_upsert[mysql_col] = Decimal(str(ksql_value))
                                    except Exception as conv_err:
                                        print(f"ERROR [Sync: {sync_key}]: Failed to convert '{ksql_col}' value '{ksql_value}' to Decimal: {conv_err}")
                                        valid_data = False; break
                                # Ajouter d'autres conversions si nécessaire (ex: String -> Datetime)
                                else:
                                    data_to_upsert[mysql_col] = ksql_value # Assumer types compatibles

                            if not valid_data: continue

                            # Exécuter l'UPSERT (Gestion DELETE non implémentée ici, nécessite détection)
                            # print(f"DEBUG Preparing UPSERT [{sync_key}]: PK={primary_key_values_tuple}, Data={data_to_upsert}")
                            if not execute_mysql_upsert_or_delete(db_conn, config, primary_key_values_tuple, data_to_upsert):
                                print(f"WARN [Sync: {sync_key}]: Database operation failed. Pausing briefly.")
                                time.sleep(error_delay)


                    except json.JSONDecodeError:
                        if header_processed: # N'afficher l'erreur que si ce n'est pas l'en-tête
                            print(f"WARN [Sync: {sync_key}]: JSON decoding failed for line: '{decoded_line}'")
                    except Exception as inner_e:
                        print(f"ERROR [Sync: {sync_key}]: Error processing line: {inner_e} - Line: '{decoded_line}'")
                        time.sleep(1)

        except requests.exceptions.RequestException as e:
            current_time = time.time()
            if current_time - last_error_time > error_delay:
                print(f"ERROR [Sync: {sync_key}]: ksqlDB connection failed: {e}. Retrying in {error_delay} seconds...")
                last_error_time = current_time
            time.sleep(error_delay)
        except Exception as e:
             current_time = time.time()
             if current_time - last_error_time > error_delay:
                 print(f"ERROR [Sync: {sync_key}]: An unexpected error occurred: {e}. Retrying in {error_delay} seconds...")
                 last_error_time = current_time
             time.sleep(error_delay)
        finally:
            pass # Garder la connexion DB ouverte pour la boucle while True

    # Cleanup (si la boucle se termine)
    if db_conn and db_conn.is_connected():
        db_conn.close()
        print(f"INFO [Sync: {sync_key}]: MySQL connection closed.")


# --- Point d'Entrée Principal ---
if __name__ == "__main__":
    print("--- Starting ksqlDB Table to MySQL Sync Script ---")

    # Valider la configuration (optionnel mais recommandé)
    for ksql_table, cfg in TABLES_TO_SYNC.items():
        if not all(k in cfg for k in ['mysql_table', 'ksql_pk_cols', 'mysql_pk_cols', 'column_mapping']):
             print(f"FATAL: Configuration error for ksqlDB table '{ksql_table}'. Missing required keys.")
             exit(1)
        if len(cfg['ksql_pk_cols']) != len(cfg['mysql_pk_cols']):
             print(f"FATAL: Configuration error for ksqlDB table '{ksql_table}'. Mismatch in PK column list lengths.")
             exit(1)

    threads = []
    print("Initializing sync threads...")
    for ksql_table, config_data in TABLES_TO_SYNC.items():
        thread = threading.Thread(
            target=sync_table_to_mysql,
            args=(KSQLDB_URL, ksql_table, config_data), # Passer toute la config
            daemon=True
        )
        threads.append(thread)
        thread.start()
        print(f"Thread started for: {ksql_table} -> {config_data['mysql_table']}")

    print("--- Sync threads running. Press Ctrl+C to stop. ---")

    try:
        while True:
            all_alive = all(t.is_alive() for t in threads)
            if not all_alive:
                 print("WARN: One or more sync threads have stopped unexpectedly.")
                 # Pourrait tenter de relancer les threads morts ici
                 time.sleep(60)
            else:
                time.sleep(5)
    except KeyboardInterrupt:
        print("\n--- Ctrl+C detected. Shutting down sync script. ---")
    print("Exiting.")