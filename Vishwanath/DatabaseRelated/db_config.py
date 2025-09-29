import psycopg2 as pg

def connection(*args, **kwargs):
    if 'host' in kwargs and kwargs['host'] == '192.168.44.4':
         return pg.connect(host="10.147.0.69", port=5432, 
                         dbname=kwargs.get('dbname'), 
                         user=kwargs.get('user'), 
                         password=kwargs.get('password'))
    elif args and isinstance(args[0], str) and '192.168.44.4' in args[0]:
        return pg.connect(host="10.147.0.69",port=5432)
    else:
        return pg.connect(*args, **kwargs)