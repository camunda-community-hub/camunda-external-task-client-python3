def str_to_list(values):
    if isinstance(values, str):
        return [values]
    return values


def get_exception_detail(exception):
    return f"{type(exception)} : {str(exception)}"


def join(list_of_values, separator):
    if list_of_values:
        return separator.join(str(v) for v in list_of_values)
    return ''
