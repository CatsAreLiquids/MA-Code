# How to use the evaluation Functions

## Retrieval Evaluation:
    Used to evaluate the RAG component of the system, either for product or transformation findability evaluation. Test files can be found in the Test Files folder, all files contain the ground truth
    The code requires you to select the test file and retriever combination that should be evaluated, additional changes to the retriver can be made in evaluation_retriever.py.
    To score the output of the run_product_both function score_product_steps should be used.


## Generation Evaluation:
    scores a test run generated with retrieval_evaluation.py, scores four diffrent variations:
    - step: product retrieval using either a step or both a step and a query as input ground truth is the "product" column
    - query: product retrieval using only the whole query as input with ground truth in the "products" column
    - function: either in for either the query or a step, for the quiery ground thruth is the transformations column for the step it is the function column

## Prototype Evaluation:
    Assuming there no generated plan exists:
    1. run generate_plan() to create a test run of the plans 
    2. run evaluate_plans() to see how the generated plans compare to the ground truth plans located in Ground Truth/plans_ground_truth.csv
    3. unpack the sql_ground_truth.zip and evaluate_results() to see how the generated plans compare to the ground truth created from the sql 
