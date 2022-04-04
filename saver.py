import json
import os
import sys
import traceback
from typing import Any, Dict, List, Optional, TextIO, Union

import click
import loguru
import numpy as np
import pandas as pd
import psycopg2
from psycopg2 import errors as pg_errors

log = loguru.logger

class UnsafeExpressionException(Exception):
    def __init__(self, *args, **kwargs):
        super().__init__(args, **kwargs)

class NpEncoder(json.JSONEncoder):
    def default(self, obj: Any) -> Any:
        if obj is None:
            return None
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return int(obj) if obj.is_integer() else float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if obj != obj:
            return None
        return str(obj)

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
                '          WHERE n.nspname = %s AND c.relname = %s'
                '      )'
                ' ) as types JOIN information_schema.columns c ON types.column = c.column_name AND table_schema = %s AND table_name = %s',
                (schema, table) * 2)
        return pd.DataFrame(cur.fetchall(), columns=[d.name for d in cur.description])

    @staticmethod
    def get_tables_list(cur: 'psycopg2.cursor', schema: Optional[str] = None) -> pd.DataFrame:
        cur.execute(f'SELECT table_schema as schema, table_name as table FROM information_schema.tables'
                " WHERE table_name NOT LIKE 'pg_%%'"
                "   AND table_schema NOT IN ('pg_catalog', 'information_schema', 'topology')" +
                (' AND table_schema = %s' if schema is not None else '') +
                ' ORDER BY table_schema, table_name',
                ((schema,) if schema is not None else None))
        return pd.DataFrame(cur.fetchall(), columns=[d.name for d in cur.description])

    @staticmethod
    def describe_table(conn_or_cur: Union['psycopg2.connection', 'psycopg2.cursor'], table: str) -> None:
        if isinstance(conn_or_cur, psycopg2.extensions.connection):
            with conn_or_cur, conn_or_cur.cursor() as cur:
                description = DatabaseDescription.get_table_description(cur, table)
        else:
            description = DatabaseDescription.get_table_description(conn_or_cur, table)
        with pd.option_context('display.max_rows', None, 'display.max_columns', None,
                'display.width', os.get_terminal_size(0).columns):  # more options can be specified also
            print(description.fillna(''))

    @staticmethod
    def list_tables(conn_or_cur: Union['psycopg2.connection', 'psycopg2.cursor'], schema: Optional[str] = None) -> None:
        if isinstance(conn_or_cur, psycopg2.extensions.connection):
            with conn_or_cur, conn_or_cur.cursor() as cur:
                description = DatabaseDescription.get_tables_list(cur, schema)
        else:
            description = DatabaseDescription.get_tables_list(conn_or_cur, schema)

        with pd.option_context('display.max_rows', None, 'display.max_columns', None,
                'display.width', os.get_terminal_size(0).columns):  # more options can be specified also
            print(description.fillna(''))

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

        return df

    @staticmethod
    def select(conn: 'psycopg2.connection', query: str):
        # Проверка на то что передан только SELECT-запрос
        stop_phrases = ['update ', 'drop ', 'insert ', 'create ', ';',
                        'alter ', 'deallocate ', 'copy ', 'move ', 'import ',
                        'reassign ', 'grant ']

        if any(stop_phrase in query.lower() for stop_phrase in stop_phrases):
            log.error('В запросе есть что-то еще, кроме только SELECT-запроса.\n'
                  'Возможно, используется одно из ключевых слов:\n'
                  '"update", "drop", "insert", "create", ";", ...')
            raise UnsafeExpressionException(f'Query seems to be unsafe')

        with conn, conn.cursor() as cur:
            try:
                cur.execute(query)
            except Exception as e:
                log.error(f'Ошибка при исполнении запроса SELECT-запросе: {e}')
                raise

            df = pd.DataFrame(cur.fetchall(), columns=[d.name for d in cur.description])

        return df


class Save:

    @staticmethod
    def to_file(df: pd.DataFrame, filename: str, geometry_column: Optional[str] = None):
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
            if geometry_column is None:
                log.error('Geometry column is not set, but is required. Falling back to "geometry"')
                geometry_column = 'geometry'
            Save.to_geojson(df, filename, geometry_column)
        elif format == 'json':
            Save.to_json(df, filename)

    @staticmethod
    def to_buffer(df: pd.DataFrame, buffer: TextIO, format: str, geometry_column: Optional[str] = None):
        if format not in ('csv', 'xlsx', 'json', 'geojson'):
            log.error(f'Format is not supported ("{format}"), switching to csv')
            format = 'csv'
        log.info(f'Saving file in {format} format')
        if format == 'csv':
            Save.to_csv(df, buffer)
        elif format == 'xlsx':
            Save.to_excel(df, buffer)
        elif format == 'geojson':
            if geometry_column is None:
                log.error('Geometry column is not set, but is required. Falling back to "geometry"')
                geometry_column = 'geometry'
            Save.to_geojson(df, buffer, geometry_column)
        elif format == 'json':
            Save.to_json(df, buffer)

    @staticmethod
    def to_geojson(df: pd.DataFrame, filename_or_buf: Union[str, TextIO], geometry_column: str = 'geometry') -> None:
        log.debug('Saving geojson' + f' to "{filename_or_buf}"' if isinstance(filename_or_buf, str) else '')
        serializable_types = ['object', 'int64', 'float64', 'bool']

        if geometry_column not in df.columns:
            log.error(f'Geometry column "{geometry_column}" is not present, aborting')
            return

        geometry_series = df[geometry_column]
        df = df.drop(geometry_column, axis=1)

        for col in set(df.columns):
            if isinstance(df[col], pd.DataFrame):
                log.warning(f'Table has more than one column with the same name: "{col}", renaming')
                r = iter(range(df.shape[1] + 1))
                df = df.rename(lambda name: name if name != col else f'{col}_{next(r)}', axis=1)
                for col_idx in range(next(r)):
                    if df[f'{col}_{col_idx}'].dtypes not in serializable_types:
                        log.warning(f'Dropping non-serializable "{col}_{col_idx}" column')
            else:
                if df[col].dtypes not in serializable_types:
                    log.warning(f'Dropping non-serializable "{col}" column')
                    df = df.drop(col, axis=1)

        geojson = {'type': 'FeatureCollection',
                'crs': {'type': 'name',
                        'properties': {
                                'name': 'urn:ogc:def:crs:EPSG::4326'
                        }
                },
                'features': [{'type': 'Feature', 'properties': dict(row), 'geometry': geometry} for (_, row), geometry in zip(df.iterrows(), geometry_series)]
        }
        if isinstance(filename_or_buf, str):
            geojson['name'] = filename_or_buf
            with open(filename_or_buf, 'w', encoding='utf-8') as file:
                json.dump(geojson, file, ensure_ascii=False, cls=NpEncoder)
        else:
            json.dump(geojson, filename_or_buf, ensure_ascii=False, cls=NpEncoder)

        log.debug('Saved')

    @staticmethod
    def to_json(df: pd.DataFrame, filename_or_buf: Union[str, TextIO]) -> None:
        log.debug(f'Saving json' + f' to "{filename_or_buf}"' if isinstance(filename_or_buf, str) else '')

        serializable_types = ['object', 'int64', 'float64', 'bool']

        for col in set(df.columns):
            if isinstance(df[col], pd.DataFrame):
                log.warning(f'Table has more than one column with the same name: "{col}", renaming')
                r = iter(range(df.shape[1] + 1))
                df = df.rename(lambda name: name if name != col else f'{col}_{next(r)}', axis=1)
                for col_idx in range(next(r)):
                    if df[f'{col}_{col_idx}'].dtypes not in serializable_types:
                        log.warning(f'Dropping non-serializable "{col}_{col_idx}" column')
            else:
                if df[col].dtypes not in serializable_types:
                    log.warning(f'Dropping non-serializable "{col}" column')
                    df = df.drop(col, axis=1)
        print(df)
        data: List[Dict[str, Any]] = [dict(row) for _, row in df.iterrows()]
        if isinstance(filename_or_buf, str):
            with open(filename_or_buf, 'w', encoding='utf-8') as file:
                json.dump(data, file, ensure_ascii=False, indent=4, cls=NpEncoder)
        else:
            json.dump(data, filename_or_buf, ensure_ascii=False, cls=NpEncoder)
        log.debug('Saved')

    @staticmethod
    def to_csv(df: pd.DataFrame, filename_or_buf: Union[str, TextIO]) -> None:
        log.debug('Saving csv' + f' to {filename_or_buf}' if isinstance(filename_or_buf, str) else '')
        df.to_csv(filename_or_buf, header=True, index=False)
        log.debug(f'Saved')

    @staticmethod
    def to_excel(df: pd.DataFrame, filename_or_buf: Union[str, TextIO]) -> None:
        log.debug('Saving excel' + f' to {filename_or_buf}' if isinstance(filename_or_buf, str) else '')
        if isinstance(filename_or_buf, str):
            df.to_excel(filename_or_buf, header=True, index=False)
        else:
            with pd.ExcelWriter(filename_or_buf) as writer:
                df.to_excel(writer, header=True, index=False)
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
@click.option('--interactive', '-i', is_flag=True, help='Launch in interactive mode')
@click.option('--verbose_level', '-v', envvar='VERBOSE_LEVEL', type=click.Choice(['ERROR', 'WARNING', 'INFO', 'DEBUG']),
        default='WARNING', help='Verbose level for the logging')
@click.option('--filename', '-f', type=str, metavar='path/to/file.[csv|xlsx|geojson|json]', default=None,
        help='Path of the file to save results (.csv, .xlsx, .geojson, .json extensions)')
@click.argument('query', type=str, metavar='query/select', required=False)
def main(db_addr: str, db_port: int, db_name: str, db_user: str, db_pass: str, geometry_column: str, use_centroids: bool,
        list_tables: bool, describe_table: Optional[str], interactive: bool, verbose_level: str,
        filename: Optional[str], query: Optional[str]) -> None:
    '''
        Execute query or select full table by name

        QUERY can be a table name or a select-query
    '''
    log.remove()
    log.add(sys.stderr, level=verbose_level)

    if geometry_column is not None and filename is None:
        log.warning('Geometry column is set, but saving to file is not configured')

    log.info(f'Connecting to {db_user}@{db_addr}:{db_port}/{db_name}')
    
    try:
        with psycopg2.connect(host=db_addr, port=db_port, dbname=db_name, user=db_user, password=db_pass, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT 1')
                assert cur.fetchone()[0] == 1, 'Error on database connection'
    except psycopg2.OperationalError as ex:
        log.error(f'Error on database connection: {ex}')
        exit(1)
    if query is not None and os.path.isfile(query):
        log.info('Query is treated as filename, reading query from file')
        try:
            with open(query, 'r') as f:
                query = f.read()
        except Exception as ex:
            log.error(f'Exception on file read: {ex}')

    if interactive:
        log.debug('Entering interactive mode')
        if list_tables or describe_table is not None or filename is not None or query is not None:
            log.warning('Interactive mode is launching, but some extra parameters (--list_tables, --describe_table or a query) are given. Ignoring')
        help = 'Commands available:' \
                '\tq, \\q, quit, exit - quit application\n' \
                '\t<query/filename> [> filename] - execute one-lined select query (and save the result to file if given)\n' \
                '\t"<query/filename>" [> filename] - execute select query (and save the result to file if given)\n' \
                '\t\\s <table_name> [> filename] - select * from table name (and save the result to file if given)\n' \
                '\t\\dt [schema] - list tables in the given schema, or in all schemas if not given\n' \
                '\t\\d [schema.]<table> - get table description\n' \
                '\t\\geometry_column, \\g - change geometry column [current: {geometry_column}]\n' \
                '\t\\use_centroids, \\c - switch centroids usage on selecting tables [current {use_centroids}]'
        print('You are in interactive mode.', help, '\thelp - show this message', sep='\n')
        while True:
            try:
                command = input('>> ')
                if command in ('q', '\\q', 'quit', 'exit'):
                    break
                elif command.startswith('\\dt'):
                    schema = command.split()[1] if ' ' in command else None
                    DatabaseDescription.list_tables(conn, schema)
                elif command.startswith('\\d'):
                    if ' ' not in command:
                        print('You must use \\d with table name after it, aborting')
                        continue
                    DatabaseDescription.describe_table(conn, command.split()[1])
                elif command.startswith('\\g'):
                    if ' ' not in command:
                        print('You must use \\g with table name after it, aborting')
                        continue
                    geometry_column = command.split()[1]
                    print(f'Switched geometry column to "{geometry_column}"')
                elif command in ('\\use_centroids', '\\c'):
                    use_centroids = not use_centroids
                    print(f'Centroid usage is swithced to: {use_centroids}')
                elif command.startswith('\\s'):
                    if ' ' not in command:
                        print('You must use \\s with table name after it, aborting')
                        continue
                    filename = None
                    table_end = len(command)
                    if '>' in command:
                        filename = command[command.rfind('>') + 1:].strip().strip('\'"')
                        table_end = command.rfind('>')
                        log.debug(f'Saving table select to file "{filename}"')

                    table_name = command[2:table_end].strip()
                    log.debug(f'Selecting table {table_name}')
                    df = Query.get_table(conn, table_name)

                    print(df)

                    if filename is not None:
                        Save.to_file(df, filename)
                elif command[0] == '"':
                    if command.find('"', 1) == -1 or command.count('"') - command.count('\\"') == 0:
                        while True:
                            try:
                                line = input('>>>"')
                                command += f' {line}'
                                if line.find('"', 1) != -1 or line.count('"') - line.count('\\"') != 0:
                                    break
                            except KeyboardInterrupt:
                                print('Ctrl+C hit, aborting query')
                                command = ''
                                break
                        if command == '':
                            continue
                    filename = None
                    query_end = command.rfind('"')
                    if '>' in command:
                        filename = command[command.rfind('>') + 1:].strip().strip('\'"')
                        query_end = command.rfind('"', 2, command.rfind('>'))
                        log.debug(f'Saving query to file "{filename}"')
                    query = command[1:query_end].strip()
                    if os.path.isfile(query):
                        log.info('Query is treated as filename, reading query from file')
                        try:
                            with open(query, 'r') as f:
                                query = f.read()
                        except Exception as ex:
                            log.error(f'Exception on file read: {ex}')
                    log.debug(f'Running query: {query}')

                    df = Query.select(conn, query)
                    print(df)

                    if filename is not None:
                        Save.to_file(df, filename, geometry_column)
                elif command == 'help':
                    print(help)
                else:
                    if '>' in command:
                        query = command[:command.find('>')].strip()
                        filename = command[command.find('>') + 1:].strip().strip('\'"')
                    else:
                        query = command
                        filename = None

                    if os.path.isfile(query):
                        log.info('Query is treated as filename, reading query from file')
                        try:
                            with open(query, 'r') as f:
                                query = f.read()
                        except Exception as ex:
                            log.error(f'Exception on file read: {ex}')

                    log.debug(f'Running query (no options left): {query}')
                    
                    df = Query.select(conn, command)
                    print(df)

                    if filename is not None:
                        Save.to_file(df, filename, geometry_column)

            except KeyboardInterrupt:
                print('Ctrl+C hit, exiting')
                break
            except pg_errors.UndefinedTable as ex:
                print(f'Table is not found: {ex.pgerror}')
            except pg_errors.UndefinedColumn as ex:
                print(f'Column is not found: {ex.pgerror}')
            except (pg_errors.UndefinedFunction, pg_errors.UndefinedParameter) as ex:
                print(f'Using undefined function: {ex.pgerror}')
            except pg_errors.SyntaxError as ex:
                print(f'Syntax error: {ex.pgerror}')
            except UnsafeExpressionException:
                print('This utility is not ment to update data, use other methods, aborting')
            except Exception as ex:
                print(f'Exception occured: {ex}')
                log.debug(traceback.format_exc())
    elif list_tables:
        log.debug('Listing tables in datbase')
        DatabaseDescription.list_tables(conn)
    elif describe_table is not None:
        log.debug(f'Describing "{describe_table}" table')
        DatabaseDescription.describe_table(conn, describe_table)
    elif query is not None:
        try:
            if query.lower().startswith('select'):
                if use_centroids:
                    log.warning('Option --use_centroids is ignored due to user query')
                df = Query.select(conn, query)
            else:
                df = Query.get_table(conn, query, use_centroids)
        except pg_errors.UndefinedTable as ex:
            print(f'Table is not found: {ex.pgerror}')
            exit(1)
        except pg_errors.UndefinedColumn as ex:
            print(f'Column is not found: {ex.pgerror}')
            exit(1)
        except (pg_errors.UndefinedFunction, pg_errors.UndefinedParameter) as ex:
            print(f'Using undefined function: {ex.pgerror}')
            exit(1)
        except pg_errors.SyntaxError as ex:
            print(f'Syntax error: {ex.pgerror}')
            exit(1)
        except UnsafeExpressionException:
            print('This utility is not ment to update data, use other methods, aborting')
            exit(1)
        except Exception as ex:
            print(f'Exception occured: {ex}')
            log.debug(traceback.format_exc())
            exit(1)

        print(df)

        if filename is not None:
            Save.to_file(df, filename, geometry_column)
    else:
        log.error('No query, -l or -d is given, nothing to be done')
        exit(1)

if __name__ == '__main__':
    main()
