from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from datetime import datetime

def _tele_send_message(**kwargs):
        import requests

        msg = ''
        if kwargs["notiftype"] == "success":
            msg = f"\U00002705 scheduler is running successfully."
        elif kwargs["notiftype"] == "fail":
            msg = f"\U0001F534 scheduler is failed."

        token_bot = ""
        chatid = ""
        url = f"https://api.telegram.org/bot{token_bot}/sendMessage?chat_id={chatid}&text={msg}&parse_mode=HTML"
        print(url)
        print(requests.get(url))

def _tele_send_document(**kwargs):
        import requests

        token_bot = ""
        chatid = ""

        # The URL for the sendDocument method
        url = f'https://api.telegram.org/bot{token_bot}/sendDocument'
        file_path = '/opt/airflow/projects/sandbox/files/log.txt'

        try:
            with open(file_path, 'rb') as file:
                files = {'document': file}
                data = {'chat_id': chatid, 'parse_mode':'HTML'}
                response = requests.post(url, data=data, files=files)
                response.raise_for_status()  # Raise an HTTPError for bad responses
                print(response.json())
        except FileNotFoundError:
            print(f'File not found: {file_path}')
        except requests.exceptions.RequestException as e:
            print(f'Failed to send {file_path}: {e}')

with DAG('tele_notif_dag', start_date=datetime(2024, 1, 1),catchup=False, tags=["sandbox"]) as dag:

        notif_telegramop_success = TelegramOperator(
            task_id="notif_telegram_success",
            telegram_conn_id='telegram_tompsairflow_bot',
            text="\U00002705 scheduler <b>{{dag.dag_id}}</b> is running successfully."
        )
        notif_telegramop_fail = TelegramOperator(
            task_id="notif_telegram_fail",
            telegram_conn_id='telegram_tompsairflow_bot',
            text="\U0001F534 scheduler <b>{{dag.dag_id}}</b> is failed"
        )
        notif_telegram_api_success =   PythonOperator(
            task_id='notif_telegram_api_success',
            python_callable = _tele_send_message,
            op_kwargs={'dagid':'{{dag.dag_id}}', 'notiftype':'success'},
            trigger_rule='all_success'
        )

        notif_telegram_api_fail = PythonOperator(
            task_id='notif_telegram_api_fail',
            python_callable = _tele_send_message,
            op_kwargs={'dagid':'{{dag.dag_id}}', 'notiftype':'fail'},
            trigger_rule='one_failed'
        )

        notif_telegram_api_doc = PythonOperator(
            task_id='notif_telegram_api_doc',
            python_callable = _tele_send_document,
            op_kwargs={'dagid':'{{dag.dag_id}}', 'notiftype':'fail'},
            trigger_rule='all_done'
        )
        

        [notif_telegram_api_success, notif_telegram_api_fail] >> notif_telegram_api_doc
