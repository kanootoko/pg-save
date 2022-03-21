import psycopg2
import pandas as pd
from numpy import nan
import json
from datetime import date
import argparse


class Properties:

    @staticmethod
    def connect(db_addr: str, db_port: int, db_name: str, db_user: str, db_pass: str):
        conn = psycopg2.connect(host=db_addr, port=db_port, dbname=db_name, user=db_user, password=db_pass,
                                options="-c search_path=maintenance,provision,public,social_stats,topology")

        return conn


class DBDesc:

    @staticmethod
    def list_db_schemas_and_tables(conn) -> None:
        with conn, conn.cursor() as cur:
            cur.execute("""SELECT table_schema, table_name
                            FROM information_schema.tables
                            WHERE table_schema != 'pg_catalog'
                            AND table_schema != 'information_schema'
                            AND table_type='BASE TABLE'
                            ORDER BY table_name""")

            tables = cur.fetchall()

            dash = '-' * 57
            print(dash)
            print('{:<15s}{:>4s}'.format('table_schema'.upper(), 'table_name'.upper()))
            print(dash)

            for row in tables:
                print('{:<15s}{:>4s}'.format(row[0], row[1]))


class TableDesc:

    @staticmethod
    def get_col_types(conn, table: str = '', select: str = '') -> list[str]:
        with conn, conn.cursor() as cur:
            cur.execute("SELECT oid, typname FROM pg_catalog.pg_type")
            type_mappings = {int(oid): typname for oid, typname in cur.fetchall()}

            if table:
                query = f'SELECT * FROM {table}'
            elif select:
                query = select

            cur.execute(f'{query} LIMIT 0')
            col_types = [type_mappings[col.type_code] for col in cur.description]

        return col_types

    @staticmethod
    def get_col_names(conn, table: str = '', select: str = '') -> list:
        with conn, conn.cursor() as cur:
            if table:
                query = f'SELECT * FROM {table}'
            elif select:
                query = select

            cur.execute(f'{query} LIMIT 0')
            col_names = [col.name for col in cur.description]

        return col_names

    @staticmethod
    def describe_table(conn, table: str):
        col_names = TableDesc.get_col_names(conn, table)
        col_types = TableDesc.get_col_types(conn, table)

        dash = '-' * 30
        print(dash)
        print('{:<20s}{:>4s}'.format('col_names'.upper(), 'col_types'.upper()))
        print(dash)

        for idx, _ in enumerate(col_names):
            print('{:<20s}{:>4s}'.format(col_names[idx], col_types[idx]))


class Query:

    # Может быть стоит еще добавить выбор столбцов или все если не указаны конкретные
    @staticmethod
    def get_table(conn, table: str, centroid_col: str = ''):
        with conn, conn.cursor() as cur:
            col_names = TableDesc.get_col_names(conn, table=table)
            col_types = TableDesc.get_col_types(conn, table=table)

            geometry_cols = list()
            geometry_cols[:] = (col_names[idx] for idx, col_type in enumerate(col_types)
                                if col_type == 'geometry')

            col_types_to_drop = ['date', 'time', 'timestamp', 'datetz', 'timetz', 'timestamptz']
            new_col_names = list()
            new_col_names[:] = (col_names[idx] for idx, col_type in enumerate(col_types)
                                if col_type not in col_types_to_drop)
            col_to_select = ','.join([str(item) for item in new_col_names])

            dropped_cols = [col for col in col_names if col not in new_col_names]

            if dropped_cols:
                print(f'Несериализуемые столбцы были отброшены:\n{dropped_cols}')

            if geometry_cols:
                for geometry_col in geometry_cols:
                    col_to_select = col_to_select.replace(geometry_col,
                                                          f'ST_AsGeoJSON({geometry_col})::jsonb')

            if centroid_col and geometry_cols:
                col_to_select = col_to_select.replace(f'ST_AsGeoJSON({centroid_col})::jsonb',
                                                      f'ST_AsGeoJSON(ST_Centroid({centroid_col}))::jsonb')

            cur.execute(f'SELECT {col_to_select} FROM {table}')
            data = cur.fetchall()

        df = pd.DataFrame(data, columns=new_col_names)
        df = df.replace({nan: None})

        return df

    @staticmethod
    def select_table(conn, select_query: str):
        # Проверка на только SELECT-запрос
        stop_phrases = ['update ', 'drop ', 'insert ', 'create table', ';',
                        'alter', 'deallocate', 'copy', 'move', 'import',
                        'reassign', 'grant']

        if any(stop_phrase in select_query.lower() for stop_phrase in stop_phrases):
            print('В запросе есть что-то еще, кроме только SELECT-запроса.\n'
                  'Возможно, используется одно из ключевых слов:\n'
                  '"update", "drop", "insert", "create table" или ";".')
            return None

        with conn, conn.cursor() as cur:

            try:
                cur.execute(f'{select_query} LIMIT 5')
            except Exception as e:
                print('Ошибка в SELECT-запросе:\n\n', e)
                raise e

            data = cur.fetchall()
            col_names = TableDesc.get_col_names(conn, select=select_query)
            df = pd.DataFrame(data, columns=col_names)

            # Удаление столбцов с неподходящим типом
            good_data_types = ['object', 'int64', 'float64', 'bool']
            col_names[:] = (col_names[idx] for idx, col_type in enumerate(df.dtypes)
                            if col_type in good_data_types)

            df = df[df.columns.intersection(col_names)]
            df = df.replace({nan: None})

        return df


class Save:

    @staticmethod
    def to_geojson(df, geometry_col: str, file_name: str = '') -> None:
        serializable_types = ['object', 'int64', 'float64', 'bool']

        d = date.today().strftime("%d.%m.%Y")
        name = file_name if file_name else 'dump_db'
        file_name = f'{name}_{d}.geojson'

        with open(file_name, 'w', encoding='utf-8') as file:
            geojson = {'type': 'FeatureCollection',
                       'name': file_name,
                       'crs': {'type': 'name',
                               'properties':
                                   {'name': 'urn:ogc:def:crs:EPSG::4326'}},
                       'features': []}

            dropped_cols = []
            for _, row in df.iterrows():
                feature = {'type': 'Feature', 'properties': {}, 'geometry': row[f'{geometry_col}']}

                for col in df.columns:
                    if col == geometry_col:
                        continue
                    elif df[col].dtypes not in serializable_types:
                        if col in dropped_cols:
                            pass
                        else:
                            dropped_cols.append(col)
                        continue
                    feature['properties'][col] = row[col]

                geojson['features'].append(feature)

            if dropped_cols:
                print(f'Несериализуемые поля были отброшены:\n{dropped_cols}')

            json.dump(geojson, file, ensure_ascii=False, indent=1, default=str)

        print('\nСохранение в geojson: успешно.')

    @staticmethod
    def to_json(df, file_name: str = '') -> None:
        serializable_types = ['object', 'int64', 'float64', 'bool']

        d = date.today().strftime("%d.%m.%Y")
        name = file_name if file_name else 'dump_db'
        file_name = f'{name}_{d}.json'

        with open(file_name, 'w', encoding='utf-8') as file:
            data = {'data': []}

            dropped_cols = []
            for _, row in df.iterrows():
                content = {}

                for col in df.columns:
                    if df[col].dtypes not in serializable_types:
                        if col in dropped_cols:
                            pass
                        else:
                            dropped_cols.append(col)
                        continue
                    content[f'{col}'] = row[f'{col}']

                data['data'].append(content)

            if dropped_cols:
                print(f'Несериализуемые поля были отброшены:\n{dropped_cols}')

            json.dump(data, file, ensure_ascii=False, indent=1, default=str)

        print('Сохранение в json: успешно.')

    @staticmethod
    def to_csv(df, file_name: str = '') -> None:
        current_date = date.today().strftime("%d.%m.%Y")
        name = file_name if file_name else 'dump_db'
        file_name = f'{name}_{current_date}.csv'

        df.to_csv(file_name, header=True, index=False)
        print('Сохранение в csv: успешно.')


# -----------
class Parser:

    @staticmethod
    def main():
        # Main parser
        parser = argparse.ArgumentParser(prog='CLI-parser', description='')

        # Group_1 "Connection to DB"
        parser_conn = parser.add_argument_group(title='Connection options')
        # parser_conn.add_argument('-db-addr', nargs='?', const=0, default='10.32.1.62', type=str.lower)
        parser_conn.add_argument('-db-addr', nargs='?', const=0, default='127.0.0.1', type=str.lower)
        parser_conn.add_argument('-db-port', nargs='?', const=0, default=5432, type=int)
        parser_conn.add_argument('-db-name', nargs='?', const=0, default='city_db_final', type=str.lower)
        # parser_conn.add_argument('-db-user', nargs='?', const=0, default='postgres', type=str.lower)
        parser_conn.add_argument('-db-user', nargs='?', const=0, default='gk', type=str.lower)
        parser_conn.add_argument('-db-pass', nargs='?', const=0, default='postgres', type=str.lower)

        # Group_2 "Query data from DB"
        parser_query = parser.add_argument_group(title='Query options')
        parser_query.add_argument('-table-name', '-tn', nargs='?', const=0, default='physical_objects',
                                  type=str.lower, help='Название таблицы')
        parser_query.add_argument('-select-query', '-sq', nargs='?', const=0, default='',
                                  type=str, help='Селект-запрос')
        parser_query.add_argument('--centroid-col', '--cc', nargs='?', const=0, default='',
                                  type=str.lower, help='Обработать столбец как центройд')

        # Group_3 "Downloading (saving) data"
        parser_saver = parser.add_argument_group(title='Saving options')
        parser_saver.add_argument('-save', '-s', dest='save', default=False, action='store_true',
                                  help='Сохранять результат запроса?')
        parser_saver.add_argument('-format', '-f', nargs='?', const=0, default='csv',
                                  choices=('csv', 'json', 'geojson'), help='Формат сохранения')
        parser_saver.add_argument('-file-name', '-fn', nargs='?', const=0, default='',
                                  type=str, help='Название файла')
        parser_saver.add_argument('--geometry-col', '--gc', nargs='?', const=1, default='geometry',
                                  type=str, help='Столбец геометрии (для geojson)')

        # Group_4 describe db (get list of tables and schemas)
        parser_schema = parser.add_argument_group(title='Describe DB (get list of tables and schemas)')
        parser_schema.add_argument('-describe-db', '-ddb', nargs='?', const=0, default='n',
                                   choices=('y', 'n'), help='y=yes, n=no')

        # Group_5 describe table (get list of elements and their types)
        parser_table = parser.add_argument_group(title="Describe table (get list of elements and their types)")
        parser_table.add_argument('-describe-table', '-dt', nargs='?', const=0, default='n',
                                  choices=('y', 'n'), help='y=yes, n=no')
        parser_table.add_argument('--table-name', '--tn', nargs='?', const=0, default='physical_objects',
                                  type=str.lower, help='Название таблицы')

        # Взятие переданных аргументов
        args = parser.parse_args()

        # Определение conn для дальнейшего использования
        conn = Parser.db_connect(args)

        if getattr(args, 'describe_db') == 'y':
            # Получить список названий таблиц в схеме
            DBDesc.list_db_schemas_and_tables(conn)

        elif getattr(args, 'describe_table') == 'y':
            TableDesc.describe_table(conn, table=getattr(args, 'table_name'))

        else:
            # Определение df для дальнейшего использования
            df = Parser.db_query(conn, args)

            # Сохранение по флагу
            if args.save:
                Parser.df_save(df, args)

    @staticmethod
    def db_connect(args):
        conn = Properties.connect(db_addr=args.db_addr,
                                  db_port=args.db_port,
                                  db_name=args.db_name,
                                  db_user=args.db_user,
                                  db_pass=args.db_pass)

        print(f'\nПодключение к {args.db_name}: успешно.\n')

        return conn

    @staticmethod
    def db_query(conn, args):
        if args.select_query:
            print('Загрузка таблицы ...\n')
            df = Query.select_table(conn, select_query=args.select_query)
            print('\n', df.head(), '\n')
            return df

        elif args.table_name:
            print('Загрузка таблицы ...\n')
            df = Query.get_table(conn, table=args.table_name, centroid_col=args.centroid_col)
            print('\n', df.head(), '\n')
            return df

    @staticmethod
    def df_save(df, args):
        if not args.file_name:
            args.file_name = args.table_name

        if args.format == 'csv':
            Save.to_csv(df, file_name=args.file_name)
        elif args.format == 'geojson':
            Save.to_geojson(df, geometry_col=args.geometry_col, file_name=args.file_name)
        elif args.format == 'json':
            Save.to_json(df, file_name=args.file_name)
        else:
            raise TypeError


if __name__ == '__main__':
    Parser.main()
