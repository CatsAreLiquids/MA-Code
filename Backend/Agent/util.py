def parse_function(llm_result,call):
    l = []
    for k ,v in llm_result.items():

        if k == "API":
            l.append(v)
        elif v is not None:
            if k != "columns":
                if v == 'False' or v == 'True':
                    l.append(f"'{k}': '{v}',")
                else:
                    l.append(f"'{k}': {v},")

            else:
                l.append(v)
        else:
            l.append("")
    return call.format(*l)
