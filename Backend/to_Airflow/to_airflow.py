import random
import string

import  template
import uuid

example = {"plans":[
{"function":"http://127.0.0.1:5200/retrieve","filter_dict":{"product":"http://127.0.0.1:5000/products/superhero/superhero",}},
{"function":"http://127.0.0.1:5200/filter","filter_dict":{"conditions":{"superhero_name":"Copycat"}}},
{"function":"http://127.0.0.1:5200/retrieve","filter_dict":{"product":"http://127.0.0.1:5000/products/superhero/race",}},
{"function":'combination', 'filter_dict': {"columns_left":"race_id","columns_right":"id","type":"equals","values":["None"]}}]}

alphabet = string.ascii_lowercase +string.digits

def random_num():
    return ''.join(random.choices(alphabet,k=8))

def _fill_task(task_nr,product_nr,function_name, func_dict,dag_id):
    tmp = template.task_template
    tmp = tmp.replace("<<name>>",f"t{task_nr}")
    tmp = tmp.replace("<<task_id>>",f"{function_name}_product_{product_nr}")
    tmp = tmp.replace("<<python_callable>>",f"{template.task_dict[function_name]}")
    tmp = tmp.replace("<<op_kwargs>>", f"{{'filter_dict':{func_dict},'product_nr':'{dag_id}_{product_nr}'}}")

    return tmp

def _fill_combination_task(task_nr,product_nrs,function_name, func_dict,dag_id):
    tmp = template.task_template
    tmp = tmp.replace("<<name>>",f"t{task_nr}")
    tmp = tmp.replace("<<task_id>>",f"combine_product_{product_nrs[0]}_product_{product_nrs[1]}")
    tmp = tmp.replace("<<python_callable>>",f"{template.task_dict[function_name]}")
    tmp = tmp.replace("<<op_kwargs>>", f"{{'filter_dict':{func_dict},'left_product':'{dag_id}_{product_nrs[0]}','right_product':'{dag_id}_{product_nrs[1]}' }}")

    return tmp

def _construct_dependencies(plan):
    anchor = 0
    parts = ""

    for i in range(len(plan)):
        step = plan[i]

        # we don't want to track the first retrieve only subsequent ones
        if step["function"] == "http://127.0.0.1:5200/retrieve" :
            anchor=i-1
        elif step["function"] == "combination":
            parts += template.dependencies.replace("<<dependency>>",f"t{anchor} >> t{i}")
            parts += template.dependencies.replace("<<dependency>>",f"t{i-1} >> t{i}")
        else:
            parts += template.dependencies.replace("<<dependency>>",f"t{i-1} >> t{i}")

    parts += template.dependencies.replace("<<dependency>>",f"t{len(plan)-1} >> t{len(plan)}")
    return parts

def _construct_tasks(plan,dag_id):
    df_nr = 0
    task_str = ""
    for i in range(len(plan)):

        step = plan[i]

        if step["function"] != "combination":
            if step["function"] == "http://127.0.0.1:5200/retrieve":
                df_nr += 1

            function_name = step["function"].split("/")[-1]
            task_str += _fill_task(i,df_nr,function_name,step["filter_dict"],dag_id)

        elif step["function"] == "combination":
            function_name = step["function"].split("/")[-1]
            task_str += _fill_combination_task(i, (df_nr-1,df_nr), function_name, step["filter_dict"], dag_id)
            df_nr += 1

    task_str += template.cleanup.replace("<<dag_id>>",f"{dag_id}").replace("<<name>>",f"t{len(plan)}")

    return task_str

def convert(plan):
    dag_id = random_num()
    file_text = ""

    file_text += template.imports
    file_text = file_text.replace("<<task_name>>",f"{dag_id}")
    file_text += _construct_tasks(plan,dag_id)
    file_text += _construct_dependencies(plan)

    with open(f"dags/{dag_id}.py","w") as file:
        file.write(file_text)

    return f"dags/{dag_id}.py"

if __name__ == "__main__":
    print(convert(example["plans"]))
    #_construct_dependencies(test["plans"])