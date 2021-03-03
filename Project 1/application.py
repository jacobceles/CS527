from flask import Flask, request, render_template
from db import ConnectMysql, ConnectRedshift

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

    col_name, content, query_time = connection.run_query(query)
    connection.disconnect()
    if col_name is not None:
        return render_template("table.html", headings=col_name, data=content,
                               time_taken="<b>Time Elapsed: " + query_time + "</b>")
    else:
        return render_template("table.html", headings=None, data=None, time_taken=query_time)


@app.route('/', methods=['GET'])
def home():
    return render_template('index.html')


if __name__ == '__main__':
    app.run(debug=True)
