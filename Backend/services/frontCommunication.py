import os
import json
from flask import Flask, request

from Backend.Agent import Agent
from Backend.to_Airflow import to_airflow

app = Flask(__name__)
port = int(os.environ.get('PORT', 5100))


@app.route('/chat', methods=['GET'])
def forward_agent():
    message = request.args.get('message')
    if message is None:
        return "Please provide input"

    df,plan = Agent.invoke_agent_remote(message)

    return {'data':df.to_json(),'plan':plan}

@app.route('/trigger_to_airflow', methods=['GET'])
def trigger_to_airflow():
    content = json.loads(request.data)

    dag_id = to_airflow.convert(content["plan"]["plans"],id=None)
    return {'dag_id':dag_id}





if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=port)
