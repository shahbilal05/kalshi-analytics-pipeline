import os
from dotenv import load_dotenv
from extract import fetch_events_since, fetch_markets_since
import snowflake.connector
import json
from tempfile import NamedTemporaryFile
from datetime import datetime

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


def fetch_last_processed_timestamp(kalshi_pipeline):
    # returns last processed timestamp from metadata table
    conn = None
    cursor = None

    try:
        conn = connect_to_snowflake()
        cursor = conn.cursor()
        
        query = """
            SELECT last_processed_ts 
            FROM RAW.ingestion_metadata 
            WHERE kalshi_pipeline = %s
        """
        cursor.execute(query, (kalshi_pipeline,))
        result = cursor.fetchone()
        
        if result and result[0]:
            print(f"Last {kalshi_pipeline} processed: {result[0].isoformat()}")
            return result[0]
        else:
            print(f"No previous run found for {kalshi_pipeline}")
            return None
            
    except Exception as e:
        print(f"Error fetching last processed timestamp: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def update_metadata(kalshi_pipeline, last_processed_ts, records_processed, status):
    # update and insert metadata for pipeline run
    conn = None
    cursor = None
    
    try:
        conn = connect_to_snowflake()
        cursor = conn.cursor()

        update_sql = """
            UPDATE RAW.ingestion_metadata
            SET 
                last_processed_ts = %s,
                last_successful_run = %s,
                records_processed = records_processed + %s,
                status = %s
            WHERE kalshi_pipeline = %s
        """
        cursor.execute(update_sql, (
            last_processed_ts,
            datetime.now() if status == "SUCCESS" else None, # did not need to pass arg
            records_processed,
            status,
            kalshi_pipeline
        ))
        
        conn.commit()
        print(f"Updated metadata for {kalshi_pipeline}")
        
    except Exception as e:
        print(f"Error updating metadata: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def write_to_temp_file(records, prefix="data"):
    temp_file = NamedTemporaryFile(mode="w", suffix=".json", prefix=prefix, delete=False)
    for record in records:
        json.dump(record, temp_file) 
        temp_file.write("\n")
    temp_file.close()
    return temp_file.name

def load_events(events):
    if not events:
        print("No events to load")
        return 0
    
    conn = None
    cursor = None

    batch_id = datetime.now().isoformat()

    try:
        conn = connect_to_snowflake()
        cursor = conn.cursor()

        temp_file = write_to_temp_file(events, prefix="events_")

        stage_name = "@my_events_stage"
        cursor.execute(f"PUT file://{temp_file} {stage_name} AUTO_COMPRESS=TRUE" )

        cursor.execute(f"""
            COPY INTO raw_events (batch_id, raw_json)
            FROM (
                SELECT '{batch_id}',
                $1
                FROM @my_events_stage
            )
            FILE_FORMAT = (TYPE = JSON)
        """)
        conn.commit()

        print(f"Loaded {len(events)} events")
        return len(events)
        
    except Exception:
        print("Error loading events.")
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

    batch_id = datetime.now().isoformat()

    try:
        conn = connect_to_snowflake()
        cursor = conn.cursor()

        temp_file = write_to_temp_file(markets, prefix="markets_")

        stage_name = "@my_markets_stage"
        cursor.execute(f"PUT file://{temp_file} {stage_name} AUTO_COMPRESS=TRUE")

        cursor.execute(f"""
            COPY INTO raw_markets (batch_id, raw_json)
            FROM (
                SELECT '{batch_id}',
                $1
                FROM @my_markets_stage
            )
            FILE_FORMAT = (TYPE = JSON)
        """)
        conn.commit()

        print(f"Loaded {len(markets)} markets")
        return len(markets)
    
    except Exception:
        print("Error loading markets.")
        if conn:
            conn.rollback()
        raise

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def run_incremental_pipeline():
    print("INCREMENTAL PIPELINE START")
    print("="*60)

    last_events_ts = fetch_last_processed_timestamp("events")
    last_markets_ts = fetch_last_processed_timestamp("markets")

    # extract new data
    events = fetch_events_since(last_events_ts)
    markets = fetch_markets_since(last_markets_ts)

    if events or markets:
        print("\nLoading to Snowflake.")
        events_loaded = load_events(events)
        markets_loaded = load_markets(markets)

        # update metadata with new timestamp
        now = datetime.now()
        if events_loaded > 0:
            update_metadata("events", now, events_loaded, "SUCCESS")
        if markets_loaded > 0:
            update_metadata("markets", now, markets_loaded, "SUCCESS")
            print("\n" + "="*60)
            print("INCREMENTAL LOAD COMPLETE")
            print(f"New events: {events_loaded}")
            print(f"New markets: {markets_loaded}")
    else:
        print("\n" + "="*60)
        print("NO NEW DATA")

if __name__ == "__main__":
    run_incremental_pipeline()