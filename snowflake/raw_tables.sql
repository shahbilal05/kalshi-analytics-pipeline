CREATE TABLE raw_events (
    event_id VARCHAR PRIMARY KEY,      
    series_id VARCHAR,                 
    category VARCHAR,                     
    title VARCHAR,                          
    strike_date TIMESTAMP,                 
    raw_json VARIANT,                       
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP 
);

CREATE TABLE raw_markets (
    market_id VARCHAR PRIMARY KEY,             
    event_id VARCHAR,                  
    status VARCHAR,                        
    close_time TIMESTAMP,                   
    raw_outcome NUMBER(5,2),                 
    predicted_raw NUMBER(5,2),                      
    raw_json VARIANT,                        
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);