from pymongo import MongoClient
from db import ConnectMysql, ConnectRedshift
from sql_keywords import mysql_keywords, generic_sql_keywords


class AutoMysql:
    def __init__(self):
        self.completions = []

        connection = ConnectMysql(host='instacart.cze09fdga760.us-east-2.rds.amazonaws.com',
                                  user='datastars', password='CS527#Datastars', database='instacart')

        query = '''
        select table_name, column_name from information_schema.columns
        where table_schema = 'instacart'
        '''

        _, results, _, _ = connection.run_query(query)
        table_names = set([r[0] for r in results])
        column_names = set([r[1] for r in results])
        combined = table_names.union(column_names)
        self.completions = mysql_keywords + list(combined)

    def get_completions(self, token):
        suggestions = filter(lambda x: x.lower().startswith(token), self.completions)
        suggestions = [{'key': s} for s in suggestions]
        return suggestions


class AutoRedshift:
    def __init__(self):
        self.completions = []

        connection = ConnectRedshift(host='redshift-cs527-group2.cebainumhmtq.us-east-1.redshift.amazonaws.com',
                                     user='datastars', password='CS527#Datastars', database='instacart', port=5439)

        query = '''
        select table_name, column_name from information_schema.columns
        where table_schema='public' and table_catalog='instacart'
        '''

        _, results, _, _ = connection.run_query(query)
        table_names = set([r[0] for r in results])
        column_names = set([r[1] for r in results])
        combined = table_names.union(column_names)
        self.completions = generic_sql_keywords + list(combined)

    def get_completions(self, token):
        suggestions = filter(lambda x: x.lower().startswith(token), self.completions)
        suggestions = [{'key': s} for s in suggestions]
        return suggestions


class AutoMongo:
    def __init__(self):
        self.completions = []
        connection = MongoClient()
        db = connection.instacart
        self.completions = generic_sql_keywords + db.list_collection_names()

    def get_completions(self, token):
        suggestions = filter(lambda x: x.lower().startswith(token), self.completions)
        suggestions = [{'key': s} for s in suggestions]
        return suggestions
