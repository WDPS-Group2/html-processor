#!/usr/bin/env python

from flask import Flask, request
app = Flask(__name__)


@app.route('/freebase/label/_search')
def search():
    return read_file("./mocks/mock-elasticsearch-output")


@app.route('/sparql', methods=['POST'])
def sparql():
    return read_file("./mocks/mock-trident-output")


def read_file(filename):
    with open(filename) as f:
        query = request.args.get('q')
        content = f.read()
        return content

if __name__ == '__main__':
    app.run(host='0.0.0.0', port='9200')