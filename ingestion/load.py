from dotenv import load_dotenv
from extract import fetch_closed_events, fetch_closed_markets
import os
import snowflake.connector
import json
from tempfile import NamedTemporaryFile

load_dotenv()

def connect_to_snowflake():
    return snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        database='KALSHI_DB',
        schema='RAW',
        autocommit=False
    )

def write_to_temp_file(records, prefix="data"):
    temp_file = NamedTemporaryFile(mode="w", suffix=".json", prefix=prefix, delete=False)
    for record in records:
        temp_file.write(json.dumps(record) + "\n")
    temp_file.close()
    return temp_file.name

def load_events(events):
    if not events:
        print("No events to load")
        return 0
    
    conn = None
    cursor = None

    try:
        conn = connect_to_snowflake()
        cursor = conn.cursor()

        temp_file = write_to_temp_file(events, prefix="events_")

        stage_name = "@%raw_events"
        cursor.execute(f"PUT file://{temp_file} {stage_name} AUTO_COMPRESS=TRUE" )

        copy_sql = """
            COPY INTO raw_events
            (event_id, series_id, category, title, strike_date, raw_json)
            FILE_FORMAT = (TYPE = JSON)
        """
        cursor.execute(copy_sql)
        conn.commit()

        print(f"Loaded {len(events)} events")
        return len(events)
        
    except Exception:
        # if load fails then roll back; works because autocommit=False
        if conn:
            conn.rollback()
        raise

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def load_markets(markets):
    if not markets:
        print("No markets to load")
        return 0
    
    conn = None
    cursor = None

    try:
        conn = connect_to_snowflake()
        cursor = conn.cursor()

        temp_file = write_to_temp_file(markets, prefix="markets_")

        stage_name = "@%raw_markets"
        cursor.execute(f"PUT file://{temp_file} {stage_name} AUTO_COMPRESS=TRUE")

        copy_sql = """
            COPY INTO raw_markets
            (market_id, event_id, status, close_time, raw_outcome, predicted_raw, raw_json)
            FILE_FORMAT = (TYPE = JSON)
        """
        cursor.exeute(copy_sql)
        conn.commit()

        print(f"Loaded {len(markets)} markets")
        return len(markets)
    
    except Exception:
        if conn:
            conn.rollback()
        raise

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


    

