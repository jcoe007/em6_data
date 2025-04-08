import os
import requests
import pandas as pd
import psycopg2
from datetime import datetime
from zoneinfo import ZoneInfo

def fetch_energy_data(url):
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def flatten_record(record):
    flattened = {
        "trading_date": record.get("trading_date"),
        "grid_zone_id": record.get("grid_zone_id"),
        "grid_zone_name": record.get("grid_zone_name")
    }
    for gen in record.get("generation_type", []):
        for key, value in gen.items():
            flattened[key] = value
    return flattened

def get_last_row(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT trading_date, bat_mwh, cg_mwh, cog_mwh, gas_mwh, geo_mwh, hyd_mwh, liq_mwh, sol_mwh, win_mwh
            FROM em6_generation_data
            ORDER BY trading_date DESC
            LIMIT 1;
        """)
        return cur.fetchone()

def insert_record(conn, record):
    columns = ', '.join(record.keys())
    placeholders = ', '.join(['%s'] * len(record))
    values = list(record.values())

    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO em6_generation_data ({columns}) VALUES ({placeholders})",
            values
        )
    conn.commit()

def main():
    # --- Setup ---
    url = "https://api.em6.co.nz/ords/em6/data_api/free/price?"
    data = fetch_energy_data(url)
    items = data.get("items", [])
    if not items:
        print("No items found in the data.")
        return

    most_recent = max(items, key=lambda r: pd.to_datetime(r.get("trading_date")))
    record = flatten_record(most_recent)
    record["trading_date"] = pd.to_datetime(record["trading_date"]).isoformat()

    # --- Connect to Supabase ---
    conn = psycopg2.connect(
        dbname=os.getenv("SUPABASE_DB"),
        user=os.getenv("SUPABASE_USER"),
        password=os.getenv("SUPABASE_PASSWORD"),
        host=os.getenv("SUPABASE_HOST"),
        port=os.getenv("SUPABASE_PORT", "5432")
    )

    try:
        last_row = get_last_row(conn)
        if last_row:
            mwh_fields = ["bat_mwh", "cg_mwh", "cog_mwh", "gas_mwh", "geo_mwh", "hyd_mwh", "liq_mwh", "sol_mwh", "win_mwh"]
            same = all(
                round(float(record.get(field, 0)), 2) == round(float(last_row[idx + 1] or 0), 2)
                for idx, field in enumerate(mwh_fields)
            )
            if same:
                print("Data source has not updated _mwh values. Skipping insert.")
                return

        insert_record(conn, record)
        print("Record inserted into Supabase.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
