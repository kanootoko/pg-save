import argparse
import json
import os
from datetime import date
from typing import Any, Dict, List, Optional

import click
import loguru
import pandas as pd
import psycopg2
from numpy import nan

log = loguru.logger

class DatabaseDescription:

    @staticmethod
    def get_table_description(cur: 'psycopg2.cursor', table: str) -> pd.DataFrame:
        if '.' in table:
            schema, table = table.split('.')
        else:
            schema = 'public'
        cur.execute('SELECT types.column, types.datatype, is_nullable, column_default AS default FROM'
                ' (SELECT a.attname AS column, pg_catalog.format_type(a.atttypid, a.atttypmod) AS datatype'
                '    FROM pg_catalog.pg_attribute a'
                '    WHERE'
                '      a.attnum > 0'
                '      AND NOT a.attisdropped'
                '      AND a.attrelid = ('
                '          SELECT c.oid FROM pg_catalog.pg_class c'
                '              LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace'
                '          WHERE n.nspname = %s AND c.relname = %s AND pg_catalog.pg_table_is_visible(c.oid)'
                '      )'
                ' ) as types JOIN information_schema.columns c ON types.column = c.column_name AND table_schema = %s AND table_name = %s',
                (schema, table) * 2)
        return pd.DataFrame(cur.fetchall(), columns=[d.name for d in cur.description]).replace({nan: None})

    @staticmethod
    def get_tables_list(cur: 'psycopg2.cursor', schema: Optional[str] = None) -> pd.DataFrame:
        cur.execute(f'SELECT table_schema as schema, table_name as table FROM information_schema.tables'
                " WHERE table_name NOT LIKE 'pg_%'"
                "   AND table_schema NOT IN ('pg_catalog', 'information_schema', 'topology')" +
                (' AND schema = %s' if schema is not None else '') +
                ' ORDER BY table_schema, table_name', ((schema,) if schema is not None else None))
        return pd.DataFrame(cur.fetchall(), columns=[d.name for d in cur.description]).replace({nan: None})

    @staticmethod
    def describe_table(cur: 'psycopg2.cursor', table: str) -> None:
        description = DatabaseDescription.get_table_description(cur, table)

        print('-' * os.get_terminal_size(0).columns)
        with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
            print(description.fillna(''))
        print('-' * os.get_terminal_size(0).columns)

    @staticmethod
    def list_tables(cur: 'psycopg2.cursor', schema: Optional[str] = None) -> None:
        description = DatabaseDescription.get_tables_list(cur, schema)

        print('-' * os.get_terminal_size(0).columns)
        with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
            print(description.fillna(''))
        print('-' * os.get_terminal_size(0).columns)

class Query:

    @staticmethod
    def get_table(conn: 'psycopg2.connection', table: str, use_centroids: bool = False) -> None:
        with conn, conn.cursor() as cur:
            description = DatabaseDescription.get_table_description(cur, table).set_index('column')
            geometry_cols = [column_name for column_name, (data_type, *_) in description.iterrows() if data_type.startswith('geometry')]

            columns_to_select = ', '.join(description.index)

            for geometry_column in geometry_cols:
                if use_centroids:
                    columns_to_select = columns_to_select.replace(geometry_column,
                            f'ST_AsGeoJSON(ST_Centroid({geometry_column}))::jsonb')
                else:
                    columns_to_select = columns_to_select.replace(geometry_column,
                            f'ST_AsGeoJSON({geometry_column})::jsonb')


            cur.execute(f'SELECT {columns_to_select} FROM {table}')

            df = pd.DataFrame(cur.fetchall(), columns=description.index)

        return df.replace({nan: None})

    @staticmethod
    def select_table(conn: 'psycopg2.connection', query: str):
        # Проверка на то что передан только SELECT-запрос
        stop_phrases = ['update ', 'drop ', 'insert ', 'create ', ';',
                        'alter ', 'deallocate ', 'copy ', 'move ', 'import ',
                        'reassign ', 'grant ']

        if any(stop_phrase in query.lower() for stop_phrase in stop_phrases):
            log.error('В запросе есть что-то еще, кроме только SELECT-запроса.\n'
                  'Возможно, используется одно из ключевых слов:\n'
                  '"update", "drop", "insert", "create", ";", ...')
            return

        with conn, conn.cursor() as cur:
            try:
                cur.execute(query)
            except Exception as e:
                log.error('Ошибка в SELECT-запросе:\n\n', e)
                return

            data = cur.fetchall()
            df = pd.DataFrame(data, columns=[d.name for d in cur.description])

        return df.replace({nan: None})


class Save:

    @staticmethod
    def to_geojson(df: pd.DataFrame, filename: str, geometry_column: str = 'geometry') -> None:
        log.debug(f'Saving geojson to {filename}')
        serializable_types = ['object', 'int64', 'float64', 'bool']

        if geometry_column not in df.columns:
            log.error(f'Geometry column "{geometry_column}" is not present, aborting')
            return

        geometry_series = df[geometry_column]
        df = df.drop(geometry_column, axis=1)

        for col in df.columns:
            if df[col].dtypes not in serializable_types:
                log.warning(f'Dropping non-serializable "{col}" column')
                df = df.drop(col, axis=1)

        geojson = {'type': 'FeatureCollection',
                'name': filename,
                'crs': {'type': 'name',
                        'properties': {
                                'name': 'urn:ogc:def:crs:EPSG::4326'
                        }
                },
                'features': [{'type': 'Feature', 'properties': dict(row), 'geometry': geometry} for (_, row), geometry in zip(df.iterrows(), geometry_series)]
        }

        with open(filename, 'w', encoding='utf-8') as file:
            json.dump(geojson, file, ensure_ascii=False, default=str)

        log.debug('Saved')

    @staticmethod
    def to_json(df: pd.DataFrame, filename: str) -> None:
        log.debug(f'Saving json to {filename}')

        serializable_types = ['object', 'int64', 'float64', 'bool']

        for col in df.columns:
            if df[col].dtypes not in serializable_types:
                log.warning(f'Dropping non-serializable "{col}" column')
                df = df.drop(col, axis=1)
        
        data: List[Dict[str, Any]] = [dict(row) for _, row in df.iterrows()]
        with open(filename, 'w', encoding='utf-8') as file:
            json.dump(data, file, ensure_ascii=False, indent=4, default=str)
        log.debug('Saved')

    @staticmethod
    def to_csv(df: pd.DataFrame, filename: str) -> None:
        log.debug(f'Saving csv to {filename}')
        df.to_csv(filename, header=True, index=False)
        log.debug(f'Saved')

    @staticmethod
    def to_excel(df: pd.DataFrame, filename: str) -> None:
        log.debug(f'Saving excel to {filename}')
        df.to_excel(filename, header=True, index=False)
        log.debug(f'Saved')

@click.command()
@click.option('--db_addr', '-H', envvar='DB_ADDR', type=str, metavar='localhost', default='localhost', help='Database host addres')
@click.option('--db_port', '-P', envvar='DB_PORT', type=int, metavar='5423', default=5432, help='Database host port')
@click.option('--db_name', '-D', envvar='DB_NAME', type=str, metavar='city_db_final', default='city_db_final', help='Databse name')
@click.option('--db_user', '-U', envvar='DB_USER', type=str, metavar='postgres', default='postgres', help='Database user')
@click.option('--db_pass', '-W', envvar='DB_PASS', type=str, metavar='postgres', default='postgres', help='Database user password')
@click.option('--geometry_column', '-g', type=str, metavar='geometry', default='geometry', help='Set column name to use as geometry')
@click.option('--use_centroids', '-c', is_flag=True, help='Load geometry columns as centroids')
@click.option('--list_tables', '-l', is_flag=True, help='List tables in database and quit')
@click.option('--describe_table', '-d', type=str, metavar='table_name', default=None, help='Describe given table and quit')
@click.option('--verbose_level', '-v', envvar='VERBOSE_LEVEL', type=click.Choice(['ERROR', 'WARNING', 'INFO', 'DEBUG']),
        default='WARNING', help='Verbose level for the logging')
@click.option('--filename', '-f', type=str, metavar='path/to/file.[csv|xlsx|geojson|json]', default=None,
        help='Path of the file to save results (.csv, .xlsx, .geojson, .json extensions)')
@click.argument('query', type=str, metavar='query/select', required=False)
def main(db_addr: str, db_port: int, db_name: str, db_user: str, db_pass: str, geometry_column: str, use_centroids: bool,
        list_tables: bool, describe_table: Optional[str], verbose_level: str, filename: Optional[str], query: Optional[str]) -> None:
    '''
        Execute query or select full table by name

        QUERY can be a table name or a select-query
    '''
    log.level(verbose_level)

    log.debug(f'Connecting to {db_user}@{db_addr}:{db_port}/{db_name}')
    
    with psycopg2.connect(host=db_addr, port=db_port, dbname=db_name, user=db_user, password=db_pass) as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT 1')
            assert cur.fetchone()[0] == 1, 'Error on database connection'

    if list_tables:
        log.debug('Lising tables in datbase')
        with conn, conn.cursor() as cur:
            DatabaseDescription.list_tables(cur)
    elif describe_table is not None:
        with conn, conn.cursor() as cur:
            DatabaseDescription.describe_table(cur, describe_table)
    elif query is not None:
        if query.lower().startswith('select'):
            if use_centroids:
                log.warning('Option --use_centroids is ignored due to user query')
            df = Query.select_table(conn, query)
        else:
            df = Query.get_table(conn, query, use_centroids)

        print(df)

        if filename is not None:
            if '.' not in filename:
                log.warning('File does not have extension, using csv')
                filename += '.csv'
            format = filename.split('.')[-1]
            if format not in ('csv', 'xlsx', 'json', 'geojson'):
                log.error(f'File has wrong extension ("{format}"), switching to .csv')
                filename += '.csv'
                format = 'csv'
            log.info(f'Saving file in {format} format')
            if format == 'csv':
                Save.to_csv(df, filename)
            elif format == 'xlsx':
                Save.to_excel(df, filename)
            elif format == 'geojson':
                Save.to_geojson(df, filename, geometry_column)
            elif format == 'json':
                Save.to_json(df, filename)
    else:
        log.error('No query, -l or -d is given, nothing to be done')
        exit(1)

if __name__ == '__main__':
    main()
