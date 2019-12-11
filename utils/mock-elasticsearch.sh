#!/usr/bin/env python

from flask import Flask, request
app = Flask(__name__)

@app.route('/freebase/label/_search')
def search():
    with open("./mock-elasticsearch-output") as mock_output_file:
        query = request.args.get('q')
        output = mock_output_file.read()
        return output

if __name__ == '__main__':
    app.run(host='0.0.0.0', port='9200')