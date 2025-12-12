import fastf1
import pandas as pd
from sqlalchemy import create_engine, text
import os
import traceback
import time

DB_CONNECTION_STR = 'postgresql://admin:admin@localhost:5432/f1_dwh'
CACHE_DIR = 'f1_cache' 

if not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR)

fastf1.Cache.enable_cache(CACHE_DIR) 

def get_db_engine():
    return create_engine(DB_CONNECTION_STR)

def get_existing_races(engine):
    existing = set()
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT year, racename FROM stage.races"))
            for row in result:
                existing.add((row[0], row[1]))
    except Exception:
        pass
    return existing

def load_session_with_retry(session, retries=3):
    for attempt in range(retries):
        try:
            session.load(weather=False, telemetry=False)
            return True
        except Exception as e:
            print(f"      ⚠️ Hiba ({attempt+1}/{retries}): {e}. Várakozás...")
            time.sleep(5)
    return False

def job_extract_f1_data(years):
    engine = get_db_engine()
    existing_races = get_existing_races(engine)
    
    for year in years:
        print(f"\n================ STARTING YEAR {year} ================")
        
        try:
            schedule = fastf1.get_event_schedule(year, include_testing=False)
            races_to_process = schedule 
        except Exception as e:
            print("Error")
            continue

        for i, race in races_to_process.iterrows():
            race_name = race['EventName']
            round_num = race['RoundNumber']
            
            if round_num == 0:
                continue

            if (year, race_name) in existing_races:
                continue

            print(f"[{round_num}] Letöltés: {race_name}...")

            try:
                session = fastf1.get_session(year, race_name, 'R')
                
                success = load_session_with_retry(session)
                if not success:
                    print("Error")
                    continue

                race_df = pd.DataFrame([{
                    'year': year, 'round': round_num, 'circuitid': race['Location'],
                    'circuitname': race['EventName'], 'location': race['Location'],
                    'country': race['Country'], 'date': str(race['EventDate']),
                    'racename': race_name
                }])
                race_df.to_sql('races', engine, schema='stage', if_exists='append', index=False)
                
                results = session.results
                if 'DriverNumber' not in results.columns:
                    results = results.reset_index()
                    if 'index' in results.columns: results = results.rename(columns={'index': 'DriverNumber'})

                col_map = {'DriverNumber': 'driverid', 'Abbreviation': 'code', 'FirstName': 'givenname', 
                           'LastName': 'familyname', 'CountryCode': 'nationality', 'GivenName': 'givenname', 
                           'FamilyName': 'familyname', 'Nationality': 'nationality'}
                final_map = {k:v for k,v in col_map.items() if k in results.columns}
                drivers_df = results[list(final_map.keys())].copy().rename(columns=final_map)
                drivers_df.to_sql('drivers', engine, schema='stage', if_exists='append', index=False)
                
                laps = session.laps.dropna(subset=['LapTime']).copy()
                laps['lapTime_ms'] = laps['LapTime'].dt.total_seconds() * 1000
                
                laps_export = pd.DataFrame()
                laps_export['racename'] = [race_name] * len(laps)
                laps_export['year'] = [year] * len(laps)
                laps_export['drivercode'] = laps['Driver']
                laps_export['teamname'] = laps['Team']
                laps_export['lapnumber'] = laps['LapNumber']
                laps_export['laptime_ms'] = laps['lapTime_ms']
                laps_export['compound'] = laps['Compound']
                laps_export['tyrelife'] = laps['TyreLife']
                laps_export['trackstatus'] = laps['TrackStatus']

                laps_export.to_sql('laps', engine, schema='stage', if_exists='append', index=False)
                existing_races.add((year, race_name))

            except Exception as e:
                print("Error")

if __name__ == "__main__":
    years = [2021]
    job_extract_f1_data(years)