import pandas as pd
import re
import requests
import json
import io
import numpy as np
from uuid import uuid4
import langchain_openai
import yaml
history = [{'role': 'user', 'content': 'HEllo'},
           {'role': 'assistant', 'content': 'Hello! How can I assist you today?'},
           {'role': 'user', 'content': 'my name is ROnja'},
           {'role': 'assistant', 'content': 'Hello Ronja! How can I assist you today?'},
           {'role': 'user', 'content': 'can you tel me my name'},
           {'role': 'assistant', 'content': "I'm sorry, but I don't have access to personal information about users unless it has been shared with me in the course of our conversation. If you tell me your name, I can remember it for the duration of our chat!"},
           {'role': 'user', 'content': 'hello'},
           {'role': 'assistant', 'content': 'Hello! How can I assist you today?'}]

df = pd.read_csv("data/EDGAR_2024_GHG/GHG_by_sector_and_country.csv")
a = [{'function':'filter','values':{'Country1':'Austria','Country2':'Germany',"min_year":2000,"max_year":2010}}]
a = {'input': 'All females customers who paid with Credit Card and are at least 38 years oldThe correct data products name is customer_data_23', 'output': '```json\n[\n    {"function": "filter", "values": {"gender": "Female", "min_age": 38, "payment_method": "Credit Card"}},\n    {"function": "sum", "values": {}}\n]\n```'}
test = [{"function":"filter","values":{"gender":"Female","min_age":38,"payment_method":"Credit Card"}},{"function":"sum","values":{}}]
print(a['output'])
agent_result = json.loads(test)
print(agent_result)