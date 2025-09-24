import requests
import os
from dotenv import load_dotenv

env_path = os.path.join(os.path.dirname(__file__), '..', '.env')
print("DEBUG ENV PATH:", env_path)  # debug path .env


load_dotenv(r'd:\Purwadhika\final_project\.env')

DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
print("DEBUG WEBHOOK:", DISCORD_WEBHOOK_URL)  # debug isi env var

def send_discord_notification(message: str):
    """
    Mengirim pesan ke Discord menggunakan Webhook.
    """
    if not DISCORD_WEBHOOK_URL:
        print("ERROR: DISCORD_WEBHOOK_URL tidak ditemukan, notifikasi tidak dikirim.")
        return  

    payload = {"content": message}
    headers = {"Content-Type": "application/json"}

    try:
        response = requests.post(DISCORD_WEBHOOK_URL, json=payload, headers=headers)
        response.raise_for_status()
        print(f"Notifikasi Discord berhasil dikirim: {message}")
    except requests.exceptions.RequestException as e:
        print(f"Gagal mengirim notifikasi ke Discord: {e}")

def task_success_callback(context):
    task_id = context.get('task_instance').task_id
    dag_id = context.get('task_instance').dag_id
    execution_date = context.get('execution_date')
    message = f"✅ Task **{task_id}** di DAG `{dag_id}` berhasil! (Execution date: {execution_date})"
    send_discord_notification(message)

def task_failure_callback(context):
    task_id = context.get('task_instance').task_id
    dag_id = context.get('task_instance').dag_id
    execution_date = context.get('execution_date')
    message = f"❌ Task **{task_id}** di DAG `{dag_id}` gagal! (Execution date: {execution_date})"
    send_discord_notification(message)

