import os
from flask import Flask, request
from Backend.Agent import MultiAgentSystem,SQLAgent
import pandas as pd

app = Flask(__name__)
port = int(os.environ.get('PORT', 5100))


@app.route('/chat', methods=['GET'])
def forward_agent():
    message = request.args.get('message')
    if message is None:
        return "Please provide input"

    df = SQLAgent.runQueryRemote(message)
    return {'data':df.to_json()}

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=port)
