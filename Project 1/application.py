from flask import Flask, request, render_template, jsonify
from db import ConnectMysql, ConnectRedshift
from sql_keywords import generic_sql_keywords

app = Flask(__name__)


@app.route('/querying', methods=["GET", "POST"])
def query_db():
    source = request.args.get('source', 'mysql')
    query = request.args.get('query', 'show tables;')

    if source.lower() == "mysql":
        connection = ConnectMysql(host='instacart.cze09fdga760.us-east-2.rds.amazonaws.com', user='datastars',
                                  password='CS527#Datastars', db='instacart')
    else:
        connection = ConnectRedshift(host='redshift-cs527-group2.cebainumhmtq.us-east-1.redshift.amazonaws.com',
                                     user='datastars', password='CS527#Datastars', database='instacart', port=5439)

    col_name, content, query_time, status = connection.run_query(query)
    connection.disconnect()
    if col_name is not None:
        return render_template("table.html", headings=col_name, data=content,
                               time_taken="<b>Time Elapsed: </b><i>" + query_time + "</i>", status=status)
    else:
        return render_template("table.html", headings=None, data=None, time_taken=query_time, status=status)


@app.route('/', methods=['GET'])
def home():
    return render_template('index.html')


@app.route('/suggest', methods=['GET'])
def suggest():
    token = request.args.get('token')
    source = request.args.get('source', 'mysql')

    token = token.lower()
    source = source.lower()

    if source == 'mysql' or source == 'redshift':
        suggestions = filter(lambda x: x.lower().startswith(token), generic_sql_keywords)
        suggestions = [{'key': value} for value in suggestions]
    else:
        suggestions = []

    body = {
        'suggestions' : suggestions
    }
    return jsonify(body)


if __name__ == '__main__':
    app.run(debug=True)
