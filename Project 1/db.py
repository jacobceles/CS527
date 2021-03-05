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
            col_info = self.cursor.description
            result = self.cursor.fetchall()
            query_time = str(int(round(time() * 1000)) - start_time) + " ms"
            if len(result) > 1000:
                result = result[:99]
                query_time += '<br>The result is too larger to transmit and display, so we limit the size to return'
            col_name = []
            for i in range(len(col_info)):
                col_name.append(col_info[i][0])
            return col_name, result, query_time
        except Exception as err:
            e = str(err)[1:-1].split(',')
            return None, None, "Query failed with error code: " + str(e[0]) + "<br>Details: " + str(e[1][2:-1])

    def disconnect(self):
        self.db.close()


class ConnectRedshift:
    def __init__(self, host="localhost", database='instacart', user='root', password=None, port=5000):
        self.con = psycopg2.connect(host=host, dbname=database, port=port, password=password, user=user)
        self.cur = self.con.cursor()

    def run_query(self, query_statement):
        try:
            start_time = int(round(time() * 1000))
            self.cur.execute(query_statement)
            col_info = self.cur.description
            result = self.cur.fetchall()
            query_time = str(int(round(time() * 1000)) - start_time) + " ms"
            if len(result) > 1000:
                result = result[:99]
                query_time += '<br>The result is too larger to transmit and display, so we limit the size to return'
            col_name = []
            for i in range(len(col_info)):
                col_name.append(col_info[i][0])
            return col_name, result, query_time
        except Exception as e:
            return None, None, str(e).capitalize()

    def disconnect(self):
        self.cur.close()
        self.con.close()
