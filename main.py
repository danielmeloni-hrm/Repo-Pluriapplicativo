import base64
import json
import os
from google.cloud import bigquery

def main_handler(event, context):
    """
    Cloud Function attivata da Pub/Sub.
    Cerca il file SQL nella cartella specifica del sito (es. sito1/query.sql).
    """
    try:
        # 1. Decodifica il messaggio ricevuto da Pub/Sub
        if 'data' not in event:
            print("Errore: Nessun dato trovato nel messaggio Pub/Sub.")
            return

        pubsub_message = base64.b64decode(event['data']).decode('utf-8')
        payload = json.loads(pubsub_message)
        
        # 2. Estrazione parametri dal JSON
        # Esempio JSON: {"sito_id": "sito1", "sql_file": "alert_vendite.sql"}
        sito_id = payload.get('sito_id')
        sql_filename = payload.get('sql_file')

        if not sito_id or not sql_filename:
            print(f"Errore: Parametri mancanti nel JSON. Ricevuto: {payload}")
            return

        # 3. Costruzione del percorso dinamico
        # Cerca in: /workspace/sito1/alert_vendite.sql
        sql_path = os.path.join(os.getcwd(), sito_id, sql_filename)
        
        print(f"Tentativo di lettura file: {sql_path}")

        # 4. Verifica esistenza e lettura del file SQL
        if not os.path.exists(sql_path):
            print(f"Errore: Il file {sql_filename} non esiste nella cartella {sito_id}.")
            return

        with open(sql_path, 'r') as f:
            query = f.read()

        # 5. Esecuzione su BigQuery
        client = bigquery.Client()
        query_job = client.query(query)
        results = query_job.result()  # Attende la fine della query

        # 6. Logica di controllo risultati
        # Se la query restituisce righe, significa che l'anomalia è presente
        row_count = results.total_rows
        
        if row_count > 0:
            print(f"ALERT [{sito_id}]: Trovate {row_count} righe di anomalie in {sql_filename}.")
            # Qui potrai aggiungere l'invio mail/slack
        else:
            print(f"CHECK [{sito_id}]: Nessuna anomalia riscontrata per {sql_filename}.")

    except Exception as e:
        print(f"Errore critico durante l'esecuzione: {str(e)}")