import ast
def sortby(df, filter_dict):
    filter_dict = ast.literal_eval(filter_dict)
    try:
        return df.sort_values(by=[filter_dict['columns']],ascending=filter_dict['ascending'])
    except:
        return df