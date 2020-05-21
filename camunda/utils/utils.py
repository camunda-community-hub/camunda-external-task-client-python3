def str_to_list(values):
    if isinstance(values, str):
        return [values]
    return values


def get_exception_detail(exception):
    return f"{type(exception)} : {str(exception)}"
