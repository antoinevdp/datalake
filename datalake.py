import requests
import json
import os
from datetime import datetime
import time
import threading # Pour gérer plusieurs feeds en parallèle

# --- Configuration ---

KSQLDB_URL = "http://localhost:8088"
# Dossier racine du Data Lake
DATA_LAKE_BASE_DIR = "data_lake"
# Liste des feeds à ingérer depuis ksqlDB
FEEDS_TO_INGEST = {
    # "nom_feed_datalake": {"ksql_name": "NOM_STREAM_OU_TABLE_KSQLDB", "type": "stream" | "table"}
    "transactions_cleaned": {"ksql_name": "TRANSACTIONS_CLEANED", "type": "stream"},
    "transactions_flattened": {"ksql_name": "TRANSACTIONS_FLATTENED", "type": "stream"},
    "transactions_blacklisted": {"ksql_name": "TRANSACTIONS_BLACKLISTED", "type": "stream"},
    "transactions_cleaned_processing": {"ksql_name": "TRANSACTIONS_CLEANED_PROCESSING", "type": "stream"},
    "transactions_cleaned_completed": {"ksql_name": "TRANSACTIONS_CLEANED_COMPLETED", "type": "stream"},
    "transactions_cleaned_pending": {"ksql_name": "TRANSACTIONS_CLEANED_PENDING", "type": "stream"},
    "transactions_cleaned_cancelled": {"ksql_name": "TRANSACTIONS_CLEANED_CANCELLED", "type": "stream"},
    "transactions_cleaned_failed": {"ksql_name": "TRANSACTIONS_CLEANED_FAILED", "type": "stream"},
    "user_transaction_totals": {"ksql_name": "USER_TRANSACTION_TOTALS", "type": "table"},
    "transactions_payment": {"ksql_name": "TRANSACTIONS_PAYMENT", "type": "table"},
    "transactions_refund": {"ksql_name": "TRANSACTIONS_REFUND", "type": "table"},
    "transactions_purchase": {"ksql_name": "TRANSACTIONS_PURCHASE", "type": "table"},
    "transactions_withdrawal": {"ksql_name": "TRANSACTIONS_WITHDRAWAL", "type": "table"},

}
# Mode de stockage principal (justifié ci-dessus)
STORAGE_MODE = "append" # Implémenté par la création de nouveaux fichiers

# --- Fonctions Utilitaires ---

def ensure_dir_exists(path):
    """Crée un dossier s'il n'existe pas."""
    os.makedirs(path, exist_ok=True)

def get_partition_path(base_dir, feed_name):
    """Calcule le chemin de la partition basé sur la date actuelle."""
    partition_date = datetime.utcnow().strftime('%Y-%m-%d')
    # Utilisation du format de partition Hive
    return os.path.join(base_dir, feed_name, f"date={partition_date}")

def write_record_to_datalake(record, feed_name, base_dir):
    """Écrit un enregistrement JSON dans un nouveau fichier dans la partition appropriée."""
    try:
        partition_path = get_partition_path(base_dir, feed_name)
        ensure_dir_exists(partition_path)

        # Générer un nom de fichier unique (timestamp en ms)
        timestamp_ms = int(time.time() * 1000)
        filename = f"data_{timestamp_ms}.json"
        filepath = os.path.join(partition_path, filename)

        # Écrire l'enregistrement au format JSON
        with open(filepath, 'w', encoding='utf-8') as f:
            # record est déjà un dict Python après json.loads()
            json.dump(record, f, ensure_ascii=False)

        # print(f"Record written to: {filepath}") # Décommenter pour le debug

    except Exception as e:
        print(f"ERROR [Feed: {feed_name}]: Failed to write record to data lake: {e}")
        print(f"Record data: {record}") # Afficher la donnée qui pose problème


def consume_and_store(ksql_url, feed_name, ksql_object_name, object_type, base_dir):
    """Se connecte à un stream/table ksqlDB et stocke les données dans le Data Lake."""
    query = f"SELECT * FROM {ksql_object_name} EMIT CHANGES;"
    print(f"INFO [Feed: {feed_name}]: Starting consumption for {object_type} '{ksql_object_name}'...")
    print(f"INFO [Feed: {feed_name}]: Executing ksqlDB query: {query}")

    url = f"{ksql_url}/query-stream"
    headers = {"Content-Type": "application/vnd.ksql.v1+json"}
    # Propriétés: 'earliest' pour lire depuis le début
    payload = json.dumps({
        "sql": query,
        "properties": {"ksql.streams.auto.offset.reset": "earliest"}
    })

    session = requests.Session() # Utiliser une session pour la persistance de connexion
    last_error_time = 0
    error_delay = 5 # Secondes avant de retenter après une erreur

    while True: # Boucle pour la résilience (reconnexion automatique)
        try:
            with session.post(url, headers=headers, data=payload, stream=True, timeout=(10, 60)) as response: # timeout (connect, read)
                response.raise_for_status() # Lève une exception pour les erreurs HTTP (4xx, 5xx)
                print(f"INFO [Feed: {feed_name}]: Connected to ksqlDB stream for '{ksql_object_name}'. Reading data...")
                last_error_time = 0 # Réinitialiser le suivi des erreurs après connexion réussi

                for line in response.iter_lines():
                    if line:
                        try:
                            # Les lignes de données sont généralement des objets JSON séparés par des nouvelles lignes.
                            # ksqlDB peut envoyer une première ligne de métadonnées (queryId, columnNames, etc.)
                            # Ou parfois envelopper les données [{...}]
                            decoded_line = line.decode('utf-8').strip()

                            # Ignorer les lignes vides ou potentiellement les messages de contrôle ksqlDB
                            if not decoded_line:
                                continue

                            # Gérer le cas où ksqlDB renvoie un tableau JSON sur une seule ligne
                            if decoded_line.startswith('[') and decoded_line.endswith(']'):
                                # Tentative de parser comme une liste JSON
                                records = json.loads(decoded_line)
                                if isinstance(records, list) and len(records) == 1 and isinstance(records[0], dict) and "queryId" in records[0]:
                                     print(f"DEBUG [Feed: {feed_name}]: Received header line: {records[0]}")
                                     continue # C'est probablement la ligne d'en-tête

                                # Si c'est une liste d'enregistrements de données
                                for record in records:
                                    print(record)
                                    if isinstance(record, dict): # Vérifier que c'est bien un objet
                                       write_record_to_datalake(record, feed_name, base_dir)
                                    else:
                                       print(f"WARN [Feed: {feed_name}]: Unexpected item in JSON array: {record}")

                            elif decoded_line.startswith('{') and decoded_line.endswith('}'):
                                # Tentative de parser comme un objet JSON unique
                                record = json.loads(decoded_line)
                                # Vérifier si c'est la ligne d'en-tête
                                if "queryId" in record and "columnNames" in record:
                                    print(f"DEBUG [Feed: {feed_name}]: Received header line: {record}")
                                    continue # Ignorer la ligne d'en-tête

                                # Sinon, c'est un enregistrement de données
                                write_record_to_datalake(record, feed_name, base_dir)
                            else:
                                print(f"WARN [Feed: {feed_name}]: Received non-JSON line: {decoded_line}")


                        except json.JSONDecodeError as json_err:
                            # Ignorer les erreurs de décodage JSON (peut être des métadonnées ou lignes partielles)
                            print(f"WARN [Feed: {feed_name}]: JSON decoding error: {json_err} - Line: '{line.decode('utf-8', errors='ignore')}'")
                        except Exception as inner_e:
                            print(f"ERROR [Feed: {feed_name}]: Error processing line: {inner_e}")
                            time.sleep(1) # Petite pause en cas d'erreur de traitement interne

        except requests.exceptions.RequestException as e:
            current_time = time.time()
            # Éviter les logs d'erreur en boucle trop rapide
            if current_time - last_error_time > error_delay:
                print(f"ERROR [Feed: {feed_name}]: ksqlDB connection/query failed: {e}. Retrying in {error_delay} seconds...")
                last_error_time = current_time
            time.sleep(error_delay)
        except Exception as e: # Capturer d'autres erreurs potentielles
             current_time = time.time()
             if current_time - last_error_time > error_delay:
                 print(f"ERROR [Feed: {feed_name}]: An unexpected error occurred: {e}. Retrying in {error_delay} seconds...")
                 last_error_time = current_time
             time.sleep(error_delay)

# --- Point d'Entrée Principal ---
if __name__ == "__main__":
    print("--- Starting Data Lake Ingestion Script ---")
    print(f"Data Lake Base Directory: {DATA_LAKE_BASE_DIR}")
    ensure_dir_exists(DATA_LAKE_BASE_DIR)

    threads = []
    print("Initializing ingestion threads...")
    for feed_name, config in FEEDS_TO_INGEST.items():
        ksql_name = config["ksql_name"]
        feed_type = config["type"]
        # Créer un thread pour chaque feed
        thread = threading.Thread(
            target=consume_and_store,
            args=(KSQLDB_URL, feed_name, ksql_name, feed_type, DATA_LAKE_BASE_DIR),
            daemon=True
        )
        threads.append(thread)
        thread.start()
        print(f"Thread started for feed: {feed_name}")

    print("--- Ingestion threads running. Press Ctrl+C to stop. ---")

    try:
        # Garder le thread principal en vie pendant que les threads daemon travaillent
        while True:
            # Vérifier si des threads sont morts (optionnel)
            all_alive = all(t.is_alive() for t in threads)
            if not all_alive:
                print("WARN: One or more ingestion threads have stopped unexpectedly.")
                # Ici, on pourrait relancer les threads morts ou investiguer.
                # Recherche des threads morts pour le log:
                for i, t in enumerate(threads):
                    if not t.is_alive():
                         # Attention: récupérer le nom du feed associé à ce thread peut être complexe
                         # sans une structure de données supplémentaire.
                         print(f"WARN: Thread index {i} is no longer alive.")
                time.sleep(60) # Vérifier toutes les minutes
            else:
                time.sleep(5)

    except KeyboardInterrupt:
        print("\n--- Ctrl+C detected. Shutting down ingestion script. ---")
        # Les threads daemon s'arrêteront automatiquement lorsque le programme principal se termine.
        print("Exiting.")