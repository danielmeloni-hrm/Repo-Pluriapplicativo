import base64
import json
import os
import smtplib
from email.message import EmailMessage
from google.cloud import bigquery

# Configurazioni Email
EMAIL_INVIO = "meloniweb@gmail.com"
PASSWORD_APP = "ettilmazxyydvusy"
DESTINATARI = ["daniel.meloni@hrm.group", "meloniwebm@gmail.com"]

def send_alert_email(sito_id, filename, count):
    """Funzione per l'invio della notifica via email."""
    msg = EmailMessage()
    msg['Subject'] = f"⚠️ ALERT BIGQUERY: Anomalie rilevate su {sito_id}"
    msg['From'] = EMAIL_INVIO
    msg['To'] = ", ".join(DESTINATARI)
    
    corpo = (f"Il sistema di monitoraggio ha rilevato delle anomalie.\n\n"
             f"Dettagli:\n"
             f"- Sito: {sito_id}\n"
             f"- Controllo eseguito: {filename}\n"
             f"- Righe rilevate: {count}\n\n"
             f"Si prega di verificare i dati su BigQuery.")
    
    msg.set_content(corpo)

    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login(EMAIL_INVIO, PASSWORD_APP)
            smtp.send_message(msg)
        print(f"Email di alert inviata con successo per {sito_id}.")
    except Exception as e:
        print(f"Errore durante l'invio dell'email: {e}")

def main_handler(event, context):
    try:
        # 1. Decodifica messaggio
        if 'data' not in event:
            return

        pubsub_message = base64.b64decode(event['data']).decode('utf-8')
        payload = json.loads(pubsub_message)
        
        sito_id = payload.get('sito_id')
        sql_filename = payload.get('sql_file')

        if not sito_id or not sql_filename:
            return

        # 2. Lettura file SQL (nella sottocartella specifica)
        sql_path = os.path.join(os.getcwd(), sito_id, sql_filename)
        
        if not os.path.exists(sql_path):
            print(f"Errore: File {sql_path} non trovato.")
            return

        with open(sql_path, 'r') as f:
            query = f.read()

        # 3. Esecuzione su BigQuery
        client = bigquery.Client()
        query_job = client.query(query)
        results = query_job.result()
        row_count = results.total_rows
        
        # 4. Logica di Alert
        if row_count > 0:
            print(f"ALERT [{sito_id}]: Trovate {row_count} righe.")
            # INVIO EMAIL
            send_alert_email(sito_id, sql_filename, row_count)
        else:
            print(f"CHECK [{sito_id}]: {sql_filename} OK (0 righe).")

    except Exception as e:
        print(f"Errore critico: {str(e)}")