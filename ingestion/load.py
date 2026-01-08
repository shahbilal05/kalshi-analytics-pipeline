import os
from dotenv import load_dotenv
from extract import fetch_closed_events, fetch_closed_markets
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
        warehouse="COMPUTE_WH",
        autocommit=False
    )

def write_to_temp_file(records, prefix="data"):
    temp_file = NamedTemporaryFile(mode="w", suffix=".json", prefix=prefix, delete=False)
    for record in records:
        json.dump(record, temp_file)  # Use json.dump instead of writing JSON strings directly
        temp_file.write("\n")
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

        stage_name = "@my_events_stage"
        cursor.execute(f"PUT file://{temp_file} {stage_name} AUTO_COMPRESS=TRUE" )

        cursor.execute("""
            COPY INTO raw_events (batch_id, raw_json)
            FROM (
                SELECT 'some-batch-id',
                $1
                FROM @my_events_stage
            )
            FILE_FORMAT = (TYPE = JSON)
        """)
        conn.commit()

        print(f"Loaded {len(events)} events")
        return len(events)
        
    except Exception:
        # reverts all changes if data load fails; autocommit is disabled
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

        stage_name = "@my_markets_stage"
        cursor.execute(f"PUT file://{temp_file} {stage_name} AUTO_COMPRESS=TRUE")

        cursor.execute("""
            COPY INTO raw_markets (batch_id, raw_json)
            FROM (
                SELECT 'some-batch-id',
                $1
                FROM @my_markets_stage
            )
            FILE_FORMAT = (TYPE = JSON)
        """)
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

def run_backfill_pipeline():
    print("BACKFILL PIPELINE START")
    print("="*60)

    print("Fetching all closed events...")
    events = fetch_closed_events()
    print(f"Fetched {len(events)} events")

    print("Fetching all closed markets...")
    markets = fetch_closed_markets()
    print(f"Fetched {len(markets)} markets")

    load_events(events)
    print(f"Total events loaded: {len(events)}")

    load_markets(markets)

    print("="*60)
    print("BACKFILL COMPLETE")

    # Total events loaded: 4332
    # Loaded 25542 markets


if __name__ == "__main__":
    run_backfill_pipeline()
    

