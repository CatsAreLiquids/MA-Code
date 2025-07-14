import ast
import pandas as pd



def sortby(df, filter_dict):
    filter_dict = ast.literal_eval(filter_dict)
    print(filter_dict)
    order = True
    if 'ascending' in filter_dict:
        if isinstance(filter_dict['ascending'],bool):
            order = filter_dict['ascending']
        else:
            if 'True' == filter_dict['ascending']:
                order = True
            elif 'False' == filter_dict['ascending']:

                order = False
    if isinstance(df,pd.Series):
        return df.sort_values(ascending=order)
    else:
        print(df.columns)
        print(df.sort_values(by=[filter_dict['columns']], ascending=order))
        try:
            print(df.sort_values(by=[filter_dict['columns']],ascending=order))
            return df.sort_values(by=[filter_dict['columns']],ascending=order)
        except:
            print("test")
            return df