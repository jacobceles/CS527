import pyodbc
import MySQLdb
import pymysql
import psycopg2
from time import time


class ConnectMysql:
    def __init__(self, host="localhost", user="root", password=None, database="instacart"):
        self.db = pymysql.connect(host=host, user=user, password=password, db=database)
        self.cursor = self.db.cursor()

    def run_query(self, query_statement):
        try:
            start_time = int(round(time() * 1000))
            self.cursor.execute(query_statement)
            query_time = str(int(round(time() * 1000)) - start_time) + " ms"

            col_info = self.cursor.description
            if col_info is None:
                query_time = "<b><i>DDL statement successfully executed!</i></b><br>"\
                             "<b>QUERY EXECUTED: </b><i>" + query_statement + "</i><br>" \
                             "<b>TIME ELAPSED: </b><i>" + query_time + "</i>"
                return None, None, query_time, "success_ddl"

            result = []
            counter = 0
            row = self.cursor.fetchone()
            while row:
                result.append(row)
                if counter < 1000:
                    row = self.cursor.fetchone()
                else:
                    query_time += \
                        '<br>The result is too large to transmit and display, so we limit the size to return.<br>'
                    break
                counter += 1

            col_name = []
            for i in range(len(col_info)):
                col_name.append(col_info[i][0])
            return col_name, result, query_time, "success_table"
        except Exception as err:
            e = str(err)[1:-1].split(',')
            error = "<b>QUERY FAILED WITH ERROR CODE: </b><i>" + str(e[0]) + \
                    "</i><br><b>DETAILS: </b><i>" + str(e[1][2:-1]) + "</i>"
            return None, None, error, "failed"

    def disconnect(self):
        self.db.commit()
        self.db.commit()
        self.db.close()


class ConnectRedshift:
    def __init__(self, host="localhost", database='instacart', user='root', password=None, port=5000):
        self.con = psycopg2.connect(host=host, dbname=database, port=port, password=password, user=user)
        self.cursor = self.con.cursor()

    def run_query(self, query_statement):
        try:
            start_time = int(round(time() * 1000))
            self.cursor.execute(query_statement)
            query_time = str(int(round(time() * 1000)) - start_time) + " ms"

            col_info = self.cursor.description
            if col_info is None:
                query_time = "<b><i>DDL statement successfully executed!</i></b><br>"\
                             "<b>QUERY EXECUTED: </b><i>" + query_statement + "</i><br>" \
                             "<b>TIME ELAPSED: </b><i>" + query_time + "</i>"
                return None, None, query_time, "success_ddl"

            result = []
            counter = 0
            row = self.cursor.fetchone()
            while row:
                result.append(row)
                if counter < 1000:
                    row = self.cursor.fetchone()
                else:
                    query_time += \
                        '<br>The result is too large to transmit and display, so we limit the size to return.<br>'
                    break
                counter += 1

            col_name = []
            for i in range(len(col_info)):
                col_name.append(col_info[i][0])
            return col_name, result, query_time, "success_table"
        except Exception as err:
            return None, None, "<b>QUERY FAILED WITH ERROR: </b><i>" + str(err).capitalize() + "</i>", "failed"

    def disconnect(self):
        self.con.commit()
        self.cursor.close()
        self.con.close()


class ConnectMongoDB:
    def __init__(self, host='localhost', database='instacart', port=3307, unix_socket='/tmp/mysql.sock'):
        self.con = MySQLdb.connect(host=host, port=port, db=database, unix_socket=unix_socket, user='root')
        self.cursor = self.con.cursor()

    def run_query(self, query_statement):
        try:
            table_list = ['aisles', 'departments', 'orders', 'order_products', 'order_products_prior', 'products']
            start_time = int(round(time() * 1000))
            table_name = ""
            try:
                if query_statement.strip().lower().startswith("select"):
                    table_name = query_statement[query_statement.strip().lower().find("from")+5:].split(" ")[0]
                if query_statement.strip().lower().startswith("drop"):
                    table_name = " ".join(query_statement.split(" ")[2:])
                    if table_name[-1] == ';':
                        table_name = table_name[:-1]
            except Exception as e:
                print(e)
                pass

            if table_name.lower() in table_list:
                if query_statement.strip().lower().startswith("select") and not query_statement.strip().lower().startswith("select into"):
                    # Read for existing tables
                    self.cursor.execute(query_statement)
                else:
                    # When messing with tables in the list and it's not a select operation
                    self.con = pyodbc.connect('DRIVER={Devart ODBC Driver for MongoDB};'
                                              'Server=127.0.0.1;'
                                              'Port=27017;'
                                              'Database=instacart')
                    self.cursor = self.con.cursor()
                    self.cursor.execute(query_statement)
                    self.con.commit()
                    if query_statement.strip().lower().startswith("drop table"):
                        table_list.remove(table_name)
            else:
                # Not in list, do whatever
                self.con = pyodbc.connect('DRIVER={Devart ODBC Driver for MongoDB};'
                                          'Server=127.0.0.1;'
                                          'Port=27017;'
                                          'Database=instacart')
                self.cursor = self.con.cursor()
                self.cursor.execute(query_statement)
                self.con.commit()

            query_time = str(int(round(time() * 1000)) - start_time) + " ms"
            col_info = self.cursor.description
            if col_info is None:
                query_time = "<b><i>DDL statement successfully executed!</i></b><br>"\
                             "<b>QUERY EXECUTED: </b><i>" + query_statement + "</i><br>" \
                             "<b>TIME ELAPSED: </b><i>" + query_time + "</i>"
                return None, None, query_time, "success_ddl"

            result = []
            counter = 0
            row = self.cursor.fetchone()
            while row:
                result.append(tuple(row))
                if counter < 1000:
                    row = self.cursor.fetchone()
                else:
                    query_time += \
                        '<br>The result is too large to transmit and display, so we limit the size to return.<br>'
                    break
                counter += 1

            col_name = []
            for i in range(len(col_info)):
                col_name.append(col_info[i][0])
            return col_name, result, query_time, "success_table"
        except Exception as err:
            return None, None, "<b>QUERY FAILED WITH ERROR: </b><i>" + str(err).capitalize() + "</i>", "failed"

    def disconnect(self):
        self.cursor.close()
        self.con.close()
