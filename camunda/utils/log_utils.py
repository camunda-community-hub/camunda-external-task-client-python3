import logging

from frozendict import frozendict

_LOGGER = logging.getLogger(__name__)
_LOGGER.addHandler(logging.NullHandler())


def log_with_context(message, context=frozendict({}), log_level="info", **kwargs):
    log_function = __get_log_function(log_level)

    log_context_prefix = __get_log_context_prefix(context)
    if log_context_prefix:
        log_function(f"{log_context_prefix} {message}", **kwargs)
    else:
        log_function(message, **kwargs)


def __get_log_context_prefix(context):
    log_context_prefix = ""
    if context:
        for k, v in context.items():
            if v:
                log_context_prefix += f"[{k}:{v}]"
    return log_context_prefix


def __get_log_function(log_level):
    switcher = {
        "debug": _LOGGER.debug,
        "info": _LOGGER.info,
        "warning": _LOGGER.warning,
        "error": _LOGGER.error,
    }
    return switcher.get(log_level, logging.info)
