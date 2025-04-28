# Rapport TP : Data Lake, Data Warehouse & Intégration Kafka

Vandeplanque Antoine


---

## 1. Introduction

Ce TP vise à construire un système de données intégrant un **Data Lake** fichier, un **Data Warehouse** MySQL, et des **Consommateurs Kafka** en Python. Il fait suite au TP KsqlDB 2 qui définissait les transformations initiales. Les technologies clés utilisées sont Kafka, Python (`kafka-python`, `mysql-connector`, `schedule`), MySQL, et un système de fichiers local pour le Data Lake.

---

## 2. Architecture

Le flux de données est le suivant :
1.  Un **Producer Python** envoie des transactions vers Kafka (`transaction_log`).
2.  ksqlDB (défini au TP2) traite ces données et génère des topics intermédiaires et des topics "changelog" pour ses Tables.
3.  Des **Consommateurs Python** lisent les topics Kafka pertinents (source et changelogs).
4.  Les données lues sont stockées dans le **Data Lake** (`./data_lake/feed_name/date=YYYY-MM-DD/`).
5.  Les données issues des topics "changelog" des Tables ksqlDB sont synchronisées (UPSERT/DELETE) dans le **Data Warehouse MySQL**.
6.  Un **Scheduler Python** exécute périodiquement des tâches de maintenance (nettoyage DL).

---

## 3. Composants Clés

*   **Data Lake (`./data_lake/`) :** Stockage fichier partitionné par feed et date (YYYY-MM-DD). Format JSON par message. Mode Append.
*   **Data Warehouse (MySQL) :** Base `my_dwh` contenant les tables :
    *   `dwh_user_transaction_totals`: Agrégats par utilisateur/type (PK: user_id, transaction_type).
    *   `dwh_windowed_transaction_totals`: Agrégats fenêtrés par type (PK: transaction_type, window_start).
    *   `dwh_datalake_permissions`: Gestion des accès au DL.
*   **Producer Kafka (`kafka_producer_transaction.py`) :** Script Python générant des messages Kafka. Volume augmenté en modifiant le nombre de messages et/ou le délai d'envoi.
*   **Consommateurs Kafka (`kafka_consumers.py`) :** Script Python utilisant `kafka-python`. Lance des threads pour lire des topics spécifiques définis dans `TOPIC_PROCESSING_CONFIG`. Écrit au DL et/ou effectue des UPSERT/DELETE dans le DWH MySQL via des fonctions réutilisables. Gère les tombstones pour les DELETEs DWH.
*   **Gouvernance & Sécurité :**
    *   **Suppression DL :** Script périodique (`scheduler_cleanup.py`) supprimant les partitions datées via `find` ou équivalent.
    *   **Permissions DL :** Table `dwh_datalake_permissions` dans MySQL pour contrôler l'accès futur.
    *   **Ajout Feed :** Mise à jour de `TOPIC_PROCESSING_CONFIG` dans le consommateur, et création/configuration éventuelle de la table DWH cible.
*   **Orchestration (`scheduler_cleanup.py`) :** Utilise la bibliothèque `schedule` pour lancer la fonction de nettoyage du Data Lake toutes les 10 minutes.

---

## 4. Guide d'Exécution

### 4.1. Prérequis

*   Python 3.7+
*   Docker & Docker Compose (pour Kafka/ksqlDB)
*   Serveur MySQL
*   Dépendances Python : `pip install kafka-python mysql-connector-python schedule python-dotenv requests`

### 4.2. Lancement

1. Démarrez Kafka, ksqlDB, MySQL.
2. Lancez les consommateurs : `python kafka_consumers.py`
3. Lancez le scheduler : `python scheduler_cleanup.py`
4. Lancez le producer : `python kafka_producer_transaction.py`

---


