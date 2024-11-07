import sys
import re
import time
from pathlib import Path
import pandas as pd
import geopandas as gpd
from sqlalchemy import (MetaData, Table)
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
import configparser
import logging


# params
config = configparser.ConfigParser(interpolation=None)
config.read('snatcher.ini')
logfile = Path('snatcher.log')
today = pd.to_datetime('today')
engine = create_engine(f"postgresql+psycopg2://{config['postgis']['user']}:{config['postgis']['password']}@{config['postgis']['host']}/{config['postgis']['database']}")
metadata = MetaData(schema='d_res')
api_url = config['api']['url']
aura_71 = ['01', '03', '07', '15', '26', '38', '42', '43', '63', '69', '71', '73', '74']

# better io for geopandas
gpd.options.io_engine = "pyogrio"

def dl_data():
    """will dl, filter and dump data
    all parameters are set outside the fct for convenience
    """
    # snatch: pandas magic !!!
    df = gpd.read_file(api_url)
    logging.info('data retrieved from source')

    # filter
    df_aura_71 = df[df['dep_code_filled'].isin(aura_71)]
    logging.info('filtering done')

    # dump
    try:
        df_aura_71.to_postgis(
        f'res_aura_71_{today.strftime("%Y%m%d")}',
        engine, schema='d_res', if_exists='fail')
        logging.info('data dumped to database')
    except ValueError:
        logging.info('table already exists: dump cancelled')

def set_latest_mv():
    """
    latest vm (named res_aura_71_latest) is wired to the latest table.
    purposes:
        - direct acces to latest data for unprivileged database user
        - join equipement families

    will delete existing latest and create new latest from scratch,
    based on latest table
    """
    # build a session object
    Session = sessionmaker(bind=engine)
    session = Session()

    # get res_aura_71_latest mv sql source
    query = """
    SELECT definition
    FROM pg_matviews
    WHERE matviewname = 'res_aura_71_latest';
    """
    result = session.execute(text(query)).fetchone()
    session.close()
    old_mv_source = result.definition[:-1]

    # replace timestamp with today
    subst = f'res_aura_71_{today.strftime("%Y%m%d")}'
    new_mv_source = re.sub(
        r"res_aura_71_\d{8}", subst, old_mv_source, 0, re.MULTILINE)
    logging.info('previous latest mv source updated')
    
    # delete old mv and create new mv
    drop_mv_sql = """
    DROP MATERIALIZED VIEW IF EXISTS "d_res"."res_aura_71_latest"
    """
    create_mv_sql = f"""
    CREATE MATERIALIZED VIEW "d_res"."res_aura_71_latest" as
    {new_mv_source}
    WITH DATA;
    -- Permissions
    ALTER TABLE d_res.res_aura_71_latest OWNER TO u_admin;
    GRANT ALL ON TABLE d_res.res_aura_71_latest TO postgres;
    GRANT ALL ON TABLE d_res.res_aura_71_latest TO u_admin;
    GRANT SELECT ON TABLE d_res.res_aura_71_latest TO u_geo;
    GRANT SELECT ON TABLE d_res.res_aura_71_latest TO urbalyon;"""
    
    # drop old mv
    with engine.connect() as connection:
        connection.execute(text(drop_mv_sql))
        connection.commit()
    connection.close()
    logging.info('previous mv dropped')

    # create new mv
    with engine.connect() as connection:
        connection.execute(text(create_mv_sql))
        connection.commit()
    connection.close()
    logging.info('new latest mv source built')

def manage_history():
    """
    will remove old tables according to rules:
        - for the current month: do nothing
        - for the current year: keep 1 table per month (last one)
        - for the past years: keep last monthly table as year's table

    tables in the current year and month must be stamped as YYYYMMDD
    tables in the current year and previous months must be stamped YYYYMM
    tables in the previous years must be stamped YYYY
    """
    # list unorganized res tables (8 digit timestamps)
    inspector = inspect(engine)
    timestamps = [
        {
            'name': t, 
            'timestamp': time.strptime(
                re.match('^res_aura_71_(?P<timestamp>\d{8})$', t).group(1),
                '%Y%m%d')}
        for t in inspector.get_table_names(schema='d_res')
        if re.match('^res_aura_71_\d{8}$', t)]
    
    # organize
    # tables from this month wont be touched
    this_month = [
        t for t in timestamps
        if t['timestamp'].tm_year == today.year
        and t['timestamp'].tm_mon == today.month]
    # tables from previous month will be wiped except last one for each month
    this_year = [
        t for t in timestamps
        if t['timestamp'].tm_year == today.year
        and t['timestamp'].tm_mon < today.month]
    # group this_year by month, select everything but last in group to be wiped
    this_year_to_wipe = [
        sorted([t for t in this_year if t['timestamp'].tm_mon==m], key=lambda x: x['timestamp'])[:-1]
        for m in set([t['timestamp'].tm_mon for t in this_year])]
    this_year_to_keep = [
        sorted([t for t in this_year if t['timestamp'].tm_mon==m], key=lambda x: x['timestamp'])[-1]
        for m in set([t['timestamp'].tm_mon for t in this_year])]
    # flatten
    this_year_wipe_flat = [c for l in this_year_to_wipe for c in l]
    # tables from previous years will be wiped except last one for each year
    previous_years = [
        t for t in timestamps
        if t['timestamp'].tm_year < today.year]
    previous_years_to_wipe = [
        sorted([t for t in previous_years if t['timestamp'].tm_year==y], key=lambda x: x['timestamp'])[:-1]
        for y in set([t['timestamp'].tm_year for t in previous_years])]
    previous_years_to_keep = [
        sorted([t for t in previous_years if t['timestamp'].tm_year==y], key=lambda x: x['timestamp'])[-1]
        for y in set([t['timestamp'].tm_year for t in previous_years])]
    # flatten
    previous_years_wipe_flat = [c for l in previous_years_to_wipe for c in l]
    logging.info('history tables identified')

    # wipe tables from previous years
    to_wipe = [Table(t['name'], metadata) for t in this_year_wipe_flat + previous_years_wipe_flat]
    for tab in to_wipe:
        tab.drop(engine)
    
    # rename history tables: previous years
    with engine.connect() as connection:
        for tbl in previous_years_to_keep:
            qry = f"ALTER TABLE d_res.{tbl['name']} RENAME TO {tbl['name'][:-4]};"
            connection.execute(text(qry))
            connection.commit()
    connection.close()

    # rename history tables: this year
    with engine.connect() as connection:
        for tbl in this_year_to_keep:
            qry = f"ALTER TABLE d_res.{tbl['name']} RENAME TO {tbl['name'][:-2]};"
            connection.execute(text(qry))
            connection.commit()
    connection.close()
    logging.info('dropped deprecated history data')


if __name__ == "__main__":

    # logging
    logging.basicConfig(
        level = logging.DEBUG,
        format = '%(asctime)s | %(levelname)s - %(message)s',
        datefmt='%m/%d/%Y %H:%M:%S',
        handlers = [
            logging.FileHandler(logfile),
            logging.StreamHandler()]
    )

    logging.info('')
    logging.info('New update')
    logging.info('----------')
    
    # download data, inject to psql
    logging.info('downloading data...')
    try:
        dl_data()
        logging.info('data successfully downloaded')
    except Exception as e:
        logging.critical(f'failed to download data: exited on {e}')
        sys.exit(1)

    # replace latest mv
    logging.info('replacing latest materialized view...')
    try:
        set_latest_mv()
        logging.info('latest materialized view replaced')
    except Exception as e:
        logging.critical(f'failed to replace latest materialized view: exited on {e}')
        sys.exit(1)

    # manage history: remove unwanted old tables
    logging.info('cleaning history data...')
    try:
        manage_history()
        logging.info('successfully cleaned history data')
    except Exception as e:
        logging.critical(f'failed to clean history data: exited on {e}')
        sys.exit(1)