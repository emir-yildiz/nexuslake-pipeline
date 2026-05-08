"""
NEXUSLAKE PIPELINE DAG
----------------------
Bronze -> Silver -> Gold servislerini sırasıyla tetikler.
Her adımda servisin tamamlanmasını bekler (Sensor).
"""

from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta
import json

default_args = {
    'owner': 'emir',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# ─── Response Checker'lar ──────────────────────────────────────────────────────

def check_trigger_response(response) -> bool:
    """
    Trigger endpoint'lerinin başarılı başladığını doğrular.
    STARTED veya ALREADY_RUNNING kabul edilir; diğerleri exception fırlatır.
    """
    try:
        body = response.json()
    except Exception:
        raise AirflowException(f"Trigger response JSON değil: {response.text}")

    status = body.get("status")

    if status in ("STARTED", "ALREADY_RUNNING"):
        return True

    raise AirflowException(
        f"Beklenmedik trigger durumu: '{status}' | Detay: {body}"
    )


def check_status_response(response) -> bool:
    """
    Sensor'ün kullandığı status checker.
    COMPLETED  → True (sensor başarıyla biter)
    FAILED     → AirflowException (sensor hemen fail olur, timeout beklemez)
    diğerleri  → False (sensor poke etmeye devam eder)
    """
    try:
        body = response.json()
    except Exception:
        raise AirflowException(f"Status response JSON değil: {response.text}")

    status = body.get("status")

    if status == "COMPLETED":
        return True

    if status == "FAILED":
        raise AirflowException(
            f"Servis FAILED durumuna geçti: {body.get('error', 'Detay yok')}"
        )

    # RUNNING, NOT_INITIALIZED → bekle
    return False


# ─── DAG ──────────────────────────────────────────────────────────────────────

with DAG(
    'nexuslake_medallion_pipeline',
    default_args=default_args,
    description='End-to-End Medallion Architecture Orchestration',
    schedule_interval=timedelta(minutes=30),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,          # Aynı anda sadece 1 run — ALREADY_RUNNING'i önler
    tags=['nexuslake', 'spark', 'medallion'],
) as dag:

    # ─── 1. BRONZE LAYER ──────────────────────────────────────────────────────

    trigger_bronze = SimpleHttpOperator(
        task_id='trigger_bronze',
        http_conn_id='bronze_service',
        endpoint='run',
        method='POST',
        data=json.dumps({"trigger_mode": "availableNow"}),
        headers={"Content-Type": "application/json"},
        response_filter=check_trigger_response,
        extra_options={"timeout": 300},      # Servis 30s içinde cevap vermezse fail
        log_response=True,
    )

    wait_for_bronze = HttpSensor(
        task_id='wait_for_bronze',
        http_conn_id='bronze_service',
        endpoint='status',
        method='GET',
        response_check=check_status_response,
        poke_interval=30,       # Bronze Kafka ingestion daha uzun sürer
        timeout=900,            # 15 dakika — Kafka lag durumları için
        mode='reschedule',      # Worker slot bloke etmez
    )

    # ─── 2. SILVER LAYER ──────────────────────────────────────────────────────

    trigger_silver = SimpleHttpOperator(
        task_id='trigger_silver',
        http_conn_id='silver_service',
        endpoint='transform',
        method='POST',
        data=json.dumps({}),
        headers={"Content-Type": "application/json"},
        response_filter=check_trigger_response,
        extra_options={"timeout": 300},
        log_response=True,
    )

    wait_for_silver = HttpSensor(
        task_id='wait_for_silver',
        http_conn_id='silver_service',
        endpoint='status',
        method='GET',
        response_check=check_status_response,
        poke_interval=30,
        timeout=600,
        mode='reschedule',
    )

    # ─── 3. GOLD LAYER ────────────────────────────────────────────────────────

    trigger_gold = SimpleHttpOperator(
        task_id='trigger_gold',
        http_conn_id='gold_service',
        endpoint='aggregate',
        method='POST',
        data=json.dumps({}),
        headers={"Content-Type": "application/json"},
        response_filter=check_trigger_response,
        extra_options={"timeout": 300},
        log_response=True,
    )

    wait_for_gold = HttpSensor(
        task_id='wait_for_gold',
        http_conn_id='gold_service',
        endpoint='status',
        method='GET',
        response_check=check_status_response,
        poke_interval=30,
        timeout=900,
        mode='reschedule',
    )

    # ─── PIPELINE FLOW ────────────────────────────────────────────────────────

    (
        trigger_bronze
        >> wait_for_bronze
        >> trigger_silver
        >> wait_for_silver
        >> trigger_gold
        >> wait_for_gold
    )