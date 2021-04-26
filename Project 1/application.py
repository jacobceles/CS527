import pandas as pd
from autocomplete import AutoMysql, AutoRedshift, AutoMongo
from db import ConnectMysql, ConnectRedshift, ConnectMongoDB
from flask import Flask, request, render_template, jsonify, make_response

global df
app = Flask(__name__)

auto_suggestors = {
    'mysql': AutoMysql(),
    'redshift': AutoRedshift(),
    'mongodb': AutoMongo()
}


@app.route('/suggest', methods=['GET'])
def suggest():
    token = request.args.get('token')
    source = request.args.get('source', 'mysql')

    token = token.lower()
    source = source.lower()

    suggestor = auto_suggestors[source]
    suggestions = suggestor.get_completions(token)

    body = {
        'suggestions': suggestions
    }
    return jsonify(body)


@app.route('/querying', methods=["GET", "POST"])
def query_db():
    source = request.args.get('source', 'mysql')
    query = request.args.get('query', 'show tables;')

    try:
        if source.lower() == "mysql":
            connection = ConnectMysql(host='instacart.cze09fdga760.us-east-2.rds.amazonaws.com',
                                      user='datastars',
                                      password='CS527#Datastars',
                                      database='instacart')
        elif source.lower() == "redshift":
            connection = ConnectRedshift(host='redshift-cs527-group2.cebainumhmtq.us-east-1.redshift.amazonaws.com',
                                         user='datastars',
                                         password='CS527#Datastars',
                                         database='instacart',
                                         port=5439)
        else:
            connection = ConnectMongoDB(host="127.0.0.1",
                                        database='instacart',
                                        port=3307,
                                        unix_socket='/tmp/mysql.sock')
    except Exception as err:
        return render_template("table.html",
                               headings=None,
                               data=None,
                               time_taken="<b>QUERY FAILED WITH ERROR: </b><i>" + str(err).capitalize() + "</i>",
                               status="failed")

    col_name, content, query_time, status = connection.run_query(query)
    connection.disconnect()
    if col_name is not None:
        # has table
        global df
        df = pd.DataFrame(content, columns=col_name)
        return render_template("table.html", headings=col_name, data=content,
                               time_taken="<b>Time Elapsed: </b><i>" + query_time + "</i>", status=status)
    else:
        # doesn't have table (DDL/Error)
        return render_template("table.html", headings=None, data=None, time_taken=query_time, status=status)


@app.route("/download/<filetype>", methods=['GET', 'POST'])
def download(filetype):
    global df
    if filetype == 'csv':
        resp = make_response(df.to_csv(header=True, index=False))
        resp.headers["Content-Disposition"] = "attachment; filename=results.csv"
        resp.headers["Content-Type"] = "text/csv"
        return resp
    elif filetype == 'json':
        resp = make_response(df.to_json(orient="table", index=False))
        resp.headers["Content-Disposition"] = "attachment; filename=results.json"
        resp.headers["Content-Type"] = "text/json"
        return resp
    elif filetype == 'html_file':
        resp = make_response(df.to_html(header=True, index=False))
        resp.headers["Content-Disposition"] = "attachment; filename=results.html"
        resp.headers["Content-Type"] = "text/html"
        return resp


@app.route('/', methods=['GET'])
def home():
    return render_template('index.html')


if __name__ == '__main__':
    app.run(debug=True)
