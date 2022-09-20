import logging

def log(func):
    def inner(*args,**kwargs):
        logging.debug(func.__name__)
        try:
           return func(*args, **kwargs)
        except e:
            logging.error(e)
            pass
    return inner