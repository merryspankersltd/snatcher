import sys
import re
import time
from pathlib import Path
import pandas as pd
import geopandas as gpd
from sqlalchemy import (MetaData, Table)
from sqlalchemy import create_engine, inspect
from sqlalchemy.sql import text
import configparser
import logging


# params
config = configparser.ConfigParser(interpolation=None)
config.read(Path('/mnt/geom/0128_Atlas_du_sport/data/snatcher/snatcher.ini'))
logfile = Path('/mnt/geom/0128_Atlas_du_sport/data/snatcher/snatcher.log')
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

def set_latest_tbl():
    """
    latest table (named res_aura_71_latest) is a copy of the
    latest timestamped table.
    purposes:
        - direct acces to latest data for unprivileged database user
        - join equipement families

    will delete existing latest and create new latest from scratch,
    based on latest table
    """

    # delete old latest and create new latest
    drop_mv_sql = """
DROP TABLE IF EXISTS "d_res"."res_aura_71_latest"
"""
    create_mv_sql = f"""
-- drop indexes explicitely
DROP INDEX IF EXISTS d_res.res_aura_71_latest_equip_numero_idx;
DROP INDEX IF EXISTS d_res.res_aura_71_latest_geometry_idx;
DROP INDEX IF EXISTS d_res.res_aura_71_latest_inst_numero_idx;
DROP INDEX IF EXISTS d_res.res_aura_71_latest_catégorie_urbalyon_2024_idx;
DROP INDEX IF EXISTS d_res.res_aura_71_latest_famille_urbalyon_2024_idx;

-- drop table explicitly
DROP TABLE IF EXISTS d_res.res_aura_71_latest CASCADE;

-- create table
create table d_res.res_aura_71_latest as
    select
        ra.*,
        tea."catégorie urbalyon 2025" as categorie_urbalyon,
        tea."famille urbalyon 2025" as famille_urbalyon,
        now() as dl_timestamp
    from d_res.res_aura_71_{today.strftime("%Y%m%d")} ra
    left join d_res.typ_eq_agence_2025 tea on ra.equip_type_code::text = tea.code::text;

-- Permissions

ALTER TABLE d_res.res_aura_71_latest OWNER TO u_admin;
GRANT ALL ON TABLE d_res.res_aura_71_latest TO u_admin;
GRANT ALL ON TABLE d_res.res_aura_71_latest TO postgres;
GRANT SELECT ON TABLE d_res.res_aura_71_latest TO u_geo;
GRANT SELECT ON TABLE d_res.res_aura_71_latest TO urbalyon;

-- indexes

CREATE UNIQUE INDEX res_aura_71_latest_equip_numero_idx ON d_res.res_aura_71_latest (equip_numero);
create INDEX res_aura_71_latest_geometry_idx ON d_res.res_aura_71_latest (geometry);
create INDEX res_aura_71_latest_inst_numero_idx ON d_res.res_aura_71_latest (inst_numero);
create INDEX res_aura_71_latest_catégorie_urbalyon_2024_idx ON d_res.res_aura_71_latest ("categorie_urbalyon");
create INDEX res_aura_71_latest_famille_urbalyon_2024_idx ON d_res.res_aura_71_latest ("famille_urbalyon");"""

    create_missings_sql = f"""
-- drop table explicitly
DROP TABLE IF EXISTS d_res.codes_eq_manquants CASCADE;

-- create table
create table d_res.codes_eq_manquants as
    select distinct
        r.equip_type_code::int "code type équipement",
        r.equip_type_name "type RES",
        r.equip_type_famille "famille RES",
        r.categorie_urbalyon "catégorie urbalyon",
        r.famille_urbalyon "famille urbalyon"
    from d_res.res_aura_71_latest r
    where
        r.equip_type_code is not null
        and r.categorie_urbalyon is null
    order by 1 asc;

-- Permissions

ALTER TABLE d_res.codes_eq_manquants OWNER TO u_admin;
GRANT ALL ON TABLE d_res.codes_eq_manquants TO u_admin;
GRANT ALL ON TABLE d_res.codes_eq_manquants TO postgres;
GRANT SELECT ON TABLE d_res.codes_eq_manquants TO u_geo;
GRANT SELECT ON TABLE d_res.codes_eq_manquants TO urbalyon;"""
    
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
        set_latest_tbl()
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