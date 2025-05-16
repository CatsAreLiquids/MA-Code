import os
from flask import Flask, request
import pandas as pd
import json
import io

from Backend.services.transformations import aggregation

app = Flask(__name__)
port = int(os.environ.get('PORT', 5200))


@app.route('/sum', methods=['PUT'])
def call_sum():

    content = json.loads(request.data)

    df = pd.read_json(io.StringIO(content['data']))
    df = aggregation.getSum(df, content['args'])

    return {'data':df.to_json()}

@app.route('/max', methods=['PUT'])
def callMax():
    pass


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=port)