import json
import time
import os
from datetime import datetime
from decimal import Decimal
import mysql.connector
from mysql.connector import Error
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

# --- Configuration (Simplifiée et Hardcodée) ---
KAFKA_BROKERS = 'localhost:9092' # Adaptez si nécessaire
TARGET_TOPIC = 'USER_TRANSACTION_TOTALS' # !! VÉRIFIEZ CE NOM DE TOPIC !!
CONSUMER_GROUP_ID = f'simple_consumer_{TARGET_TOPIC}_{int(time.time())}' # Unique pour relire

DATA_LAKE_BASE_DIR = "data_lake"
DL_FEED_NAME = 'user_transaction_totals_changelog' # Nom du dossier DL pour ce topic

MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_DB = "my_dwh"
MYSQL_USER = "your_mysql_user" # À remplacer
MYSQL_PASSWORD = "your_mysql_password" # À remplacer

# Configuration spécifique pour la table DWH cible
DWH_CONFIG = {
    "mysql_table": "dwh_user_transaction_totals",
    "ksql_pk_cols": ["USER_ID", "TRANSACTION_TYPE"], # Clés attendues dans la valeur ou la clé Kafka
    "mysql_pk_cols": ["user_id", "transaction_type"],
    "column_mapping": {"TOTAL_SPENT": "total_spent"}
}

# --- Fonctions Utilitaires (Minimales) ---
def ensure_dir_exists(path):
    os.makedirs(path, exist_ok=True)

def write_to_dl(value, feed_name, base_dir):
    """Écrit un message (dict) dans un fichier JSON du Data Lake."""
    if value is None: return # Ignorer les tombstones
    try:
        partition_date = datetime.utcnow().strftime('%Y-%m-%d')
        partition_path = os.path.join(base_dir, feed_name, f"date={partition_date}")
        ensure_dir_exists(partition_path)
        filepath = os.path.join(partition_path, f"data_{int(time.time()*1000)}.json")
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(value, f, ensure_ascii=False)
    except Exception as e:
        print(f"ERROR [DL]: Failed writing {value}: {e}")

def upsert_or_delete_dwh(connection, config, pk_values, data_map):
    """Exécute UPSERT ou DELETE dans MySQL (version simplifiée)."""
    if not connection or not connection.is_connected() or not isinstance(pk_values, tuple):
        print(f"ERROR [DWH]: Connection issue or invalid PK: {pk_values}")
        return False

    cursor = None
    success = False
    try:
        cursor = connection.cursor()
        where_clause = " AND ".join([f"`{col}` = %s" for col in config['mysql_pk_cols']])

        if data_map is None: # DELETE
            sql = f"DELETE FROM `{config['mysql_table']}` WHERE {where_clause}"
            cursor.execute(sql, pk_values)
        else: # UPSERT
            cols = config['mysql_pk_cols'] + list(data_map.keys())
            placeholders = ["%s"] * len(cols)
            updates = ", ".join([f"`{col}`=VALUES(`{col}`)" for col in data_map.keys()])
            sql = f"INSERT INTO `{config['mysql_table']}` ({', '.join([f'`{c}`' for c in cols])}) VALUES ({','.join(placeholders)}) ON DUPLICATE KEY UPDATE {updates}"
            cursor.execute(sql, pk_values + tuple(data_map.values()))

        connection.commit()
        success = True
    except Error as e:
        print(f"ERROR [DWH] PK {pk_values}: MySQL Error: {e}")
        if connection and connection.is_connected(): connection.rollback()
    finally:
        if cursor: cursor.close()
    return success

# --- Logique Principale du Consommateur ---
if __name__ == "__main__":
    print(f"--- Starting Simple Kafka Consumer for Topic: {TARGET_TOPIC} ---")
    ensure_dir_exists(DATA_LAKE_BASE_DIR)

    # Connexion MySQL (une seule fois)
    db_conn = None
    try:
        db_conn = mysql.connector.connect(
            host=MYSQL_HOST, port=MYSQL_PORT, database=MYSQL_DB,
            user=MYSQL_USER, password=MYSQL_PASSWORD, raw=False, use_pure=False
        )
        print("MySQL connection successful.")
    except Error as e:
        print(f"FATAL: MySQL connection failed: {e}. Exiting.")
        exit(1)

    # Connexion Kafka
    consumer = None
    try:
        print(f"Connecting to Kafka brokers: {KAFKA_BROKERS}...")
        consumer = KafkaConsumer(
            TARGET_TOPIC,
            bootstrap_servers=KAFKA_BROKERS,
            group_id=CONSUMER_GROUP_ID,
            auto_offset_reset='earliest',
            value_deserializer=lambda v: json.loads(v.decode('utf-8')) if v else None,
            key_deserializer=lambda k: json.loads(k.decode('utf-8')) if k and k.startswith(b'{') else (k.decode('utf-8') if k else None),
            consumer_timeout_ms=-1 # Bloque indéfiniment jusqu'au prochain message
        )
        print(f"Kafka consumer connected. Reading topic '{TARGET_TOPIC}'...")

        # Boucle de consommation
        for message in consumer:
            print(f"Received offset={message.offset} key={message.key} | value_is_None={message.value is None}")

            # 1. Écrire au Data Lake
            write_to_dl(message.value, DL_FEED_NAME, DATA_LAKE_BASE_DIR)

            # 2. Traiter pour le Data Warehouse
            is_delete = message.value is None
            pk_values_tuple = None
            data_map = None

            try:
                # Extraire la clé primaire
                if is_delete:
                    # Pour DELETE, utiliser la clé Kafka (qui doit être JSON pour ce topic)
                    if isinstance(message.key, dict):
                        pk_values_tuple = tuple(message.key.get(k) for k in DWH_CONFIG['ksql_pk_cols'])
                    else: print(f"WARN [DWH]: Tombstone key is not a dict: {message.key}")
                else:
                    # Pour UPSERT, utiliser les champs de la valeur Kafka
                    pk_values_tuple = tuple(message.value.get(k) for k in DWH_CONFIG['ksql_pk_cols'])
                    # Préparer les données à mettre à jour/insérer
                    data_map = {}
                    for ksql_col, mysql_col in DWH_CONFIG['column_mapping'].items():
                        if ksql_col in message.value:
                            # Conversion simple (Decimal)
                            val = message.value[ksql_col]
                            data_map[mysql_col] = Decimal(str(val)) if val is not None and mysql_col=="total_spent" else val

                # Exécuter l'opération DWH si la clé primaire est valide
                if pk_values_tuple and None not in pk_values_tuple:
                    upsert_or_delete_dwh(db_conn, DWH_CONFIG, pk_values_tuple, data_map)
                else:
                    print(f"WARN [DWH]: Invalid or incomplete PK extracted. Key: {message.key}, Value: {message.value}")

            except Exception as processing_error:
                print(f"ERROR [Processing] Offset {message.offset}: {processing_error}")

    except NoBrokersAvailable:
        print(f"FATAL: Cannot connect to Kafka brokers at {KAFKA_BROKERS}.")
    except KeyboardInterrupt:
        print("\nCtrl+C detected. Shutting down.")
    except Exception as e:
        print(f"FATAL: An unexpected error occurred: {e}")
    finally:
        if consumer:
            consumer.close()
            print("Kafka consumer closed.")
        if db_conn and db_conn.is_connected():
            db_conn.close()
            print("MySQL connection closed.")
        print("Exiting.")