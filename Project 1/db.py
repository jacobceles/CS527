import pyodbc
import pymysql
import psycopg2
from time import time


class ConnectMysql:
    def __init__(self, host="localhost", user="root", password=None, db="instacart"):
        self.db = pymysql.connect(host=host, user=user, password=password, db=db)
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
        except Exception as e:
            return None, None, "<b>QUERY FAILED WITH ERROR: </b><i>" + str(e).capitalize() + "</i>", "failed"

    def disconnect(self):
        self.con.commit()
        self.cursor.close()
        self.con.close()


class ConnectMongoDB:
    def __init__(self, server="127.0.0.1", database='instacart', port=27017):
        self.con = pyodbc.connect('DRIVER={Devart ODBC Driver for MongoDB};'
                                  'Server=' + server + ';Port=' + str(port) +
                                  ';Database=' + database)
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

            results = [tuple(rows) for rows in result]
            col_name = []
            for i in range(len(col_info)):
                col_name.append(col_info[i][0])
            return col_name, results, query_time, "success_table"
        except Exception as err:
            e = str(err)[1:-1].split(',')
            error = "<b>QUERY FAILED WITH ERROR CODE: </b><i>" + str(e[0]) + \
                    "</i><br><b>DETAILS: </b><i>" + str(e[1][2:-1]) + "</i>"
            return None, None, error, "failed"

    def disconnect(self):
        self.con.commit()
        self.cursor.close()
        self.con.close()
