import schedule
import time
import os
import subprocess # Pour lancer la commande de nettoyage
from datetime import datetime

DATA_LAKE_BASE_DIR = "data_lake"
RETENTION_DAYS = 90
CLEANUP_COMMAND_TEMPLATE = "find {path} -type d -mtime +{days} -print -exec rm -rf {{}} \;"

def run_datalake_cleanup():
    """Exécute la commande de nettoyage du Data Lake."""
    print(f"[{datetime.now()}] --- Running Data Lake Cleanup (Retention: {RETENTION_DAYS} days) ---")
    # Itérer sur chaque 'feed' dans le data lake
    if not os.path.isdir(DATA_LAKE_BASE_DIR):
        print(f"WARN: Data Lake directory '{DATA_LAKE_BASE_DIR}' not found.")
        return

    cleaned_count = 0
    for feed_dir in os.listdir(DATA_LAKE_BASE_DIR):
        feed_path = os.path.join(DATA_LAKE_BASE_DIR, feed_dir)
        if os.path.isdir(feed_path):
             # Cibler les sous-dossiers de partition date=YYYY-MM-DD
             # La commande find recherche directement dans le dossier du feed
             # Note: La recherche par date (-mtime) se base sur la date de modification du dossier.
             # On cible le dossier du feed pour que find cherche les 'date=...' à l'intérieur
             full_command = CLEANUP_COMMAND_TEMPLATE.format(path=feed_path, days=RETENTION_DAYS)
             print(f"Executing for feed '{feed_dir}': {full_command}")
             try:
                 # Utiliser shell=True est pratique mais comporte des risques de sécurité si le chemin est non fiable.
                 # Pour ce TP interne, c'est acceptable.
                 process = subprocess.run(full_command, shell=True, capture_output=True, text=True, check=False)
                 if process.stdout:
                     print("Deleted paths:")
                     print(process.stdout)
                     cleaned_count += len(process.stdout.strip().split('\n'))
                 if process.stderr:
                     print(f"Error during cleanup for feed '{feed_dir}': {process.stderr}")
             except Exception as e:
                 print(f"Failed to execute cleanup command for feed '{feed_dir}': {e}")

    print(f"[{datetime.now()}] --- Cleanup Finished. Approx {cleaned_count} partition folders deleted. ---")


# --- Planification ---
print("--- Scheduler Started ---")
# Planifier l'exécution de la fonction toutes les 10 minutes
schedule.every(10).minutes.do(run_datalake_cleanup)

# Exécuter une fois immédiatement au démarrage (optionnel)
print("Running initial cleanup...")
run_datalake_cleanup()
print("Initial cleanup finished. Waiting for scheduled runs...")

# Boucle pour maintenir le scheduler actif
while True:
    schedule.run_pending()
    time.sleep(60) # Vérifier chaque minute s'il y a une tâche à lancer