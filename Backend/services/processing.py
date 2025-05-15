import os
from flask import Flask, request
from Backend.Agent import MultiAgentSystem,SQLAgent
import pandas as pd

app = Flask(__name__)
port = int(os.environ.get('PORT', 5200))


@app.route('/sum', methods=['PUT'])
def call_sum():
    message = request.args.get('message')
    if message is None:
        return "Please provide input"

    df = MultiAgentSystem.runQueryRemote(message)
    return {'data':df.to_json()}

@app.route('/max', methods=['PUT'])
def callMax():
    pass


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=port)