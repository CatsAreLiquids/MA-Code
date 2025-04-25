import pandas as pd
import re
import requests
import json
import io
import numpy as np
from uuid import uuid4
import langchain_openai
history = [{'role': 'user', 'content': 'HEllo'},
           {'role': 'assistant', 'content': 'Hello! How can I assist you today?'},
           {'role': 'user', 'content': 'my name is ROnja'},
           {'role': 'assistant', 'content': 'Hello Ronja! How can I assist you today?'},
           {'role': 'user', 'content': 'can you tel me my name'},
           {'role': 'assistant', 'content': "I'm sorry, but I don't have access to personal information about users unless it has been shared with me in the course of our conversation. If you tell me your name, I can remember it for the duration of our chat!"},
           {'role': 'user', 'content': 'hello'},
           {'role': 'assistant', 'content': 'Hello! How can I assist you today?'}]