"""
A simple module to redirect PostgreSQL connections.
Import this at the start of your script.

Usage:
    import pg_redirect
"""
import psycopg2 as pg

_original_connect = pg.connect
def _redirected_connect(*args, **kwargs):
    if 'host' in kwargs and kwargs['host'] == '192.168.44.4':
        print(f"Redirecting connection from 192.168.44.4 to 10.147.0.69")
        kwargs['host'] = '10.147.0.69'
    elif args and isinstance(args[0], str) and '192.168.44.4' in args[0]:
        args = list(args)
        args[0] = args[0].replace('192.168.44.4', '10.147.0.69')
        args = tuple(args)
    return _original_connect(*args, **kwargs)

pg.connect = _redirected_connect
# print("Database redirection active: 192.168.44.4 → 10.147.0.69")