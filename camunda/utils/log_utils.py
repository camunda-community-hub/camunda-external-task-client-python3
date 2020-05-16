import logging


def log_with_context(message, context={}, log_level='info', **kwargs):
    log_context_prefix = ""
    for k, v in context.items():
        if v:
            log_context_prefix += f"[{k}:{v}]"

    log_function = __get_log_function(log_level)
    log_function(f"{log_context_prefix} {message}", **kwargs)


def __get_log_function(log_level):
    switcher = {
        'info': logging.info,
        'warning': logging.warning,
        'error': logging.error
    }
    return switcher.get(log_level, logging.info)
