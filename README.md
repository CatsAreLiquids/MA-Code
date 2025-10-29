# Prerequisits:
- Airflow needs to be installed and configured in such a way that it looks for dags in the Backend/to_Airflow/dags folder.
It otherwise cant track the dags and they need to be added manually
- Set up an env file with the credentials to access the LLMs
- install the requirments.txt
- install the postgres vector store, other vectorstores work as well but require code changes

# Metadata Generation
To generate metadata the prototypes expects the .csv files in the data folder sorted into collections.
Use the create_catalog function in Backend/data/createCatalog to create or reset the catalog files.
They can be filled with new metadata with the enrich_catalog functrion in the same file.

To add them to the vectorstore use the corresponding functions so either add_products, add_function or add_collection in the Backend/RAG/vector_db.py


# Running the prototype

## Via the front end
- start all three microservices: data_mesh, frontCommunication and transformation_sever
- ensure your vector store is up and running
- run the streamlit frontend with streamlit run frontend/app.py
- navigate to http://localhost:8501/ and the chat is ready to go 

## Without the frontend
- start the data_mesh and transformation_sever microservice
- navigate to Backend/Agent.py and follow the instructions in the main


## Airflow Conversion
 For requests via the frontend the conversion is automatically triggerd to convert an otherwise obtained plan use the 
Backend/to_Airflow/to_airflow.py and replace the example plan.


# Testing
Testing is split into findability evaluation, which only tests one aspect, and full prototype testing. 
For prototype testing, both the data_mesh and transformation_sever microservice need to be active, use generate plan to start a new testrun
Which can be evaluated with evaluate_run and test_run 
Findability testing is split into retrieval evlauation and generation evaluation, simply include the relevant test file and the testing type.

# Important Files
    - prompts.yml lists all prompts employed in the prototype and the evaluation in one convienient file
    - Backend/evaluation/Experiment Results/ contains the results reported in the paper
    - Backend/evaluation/Ground Truth/ contains the ground truth for the Bird mini-dev sqls and the ground truth plans
    - Backend/to_Airflow/dags/ contains the converted plans from the LLM+Evidence run, named after the corresponding query