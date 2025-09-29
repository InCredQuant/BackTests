import sys
import importlib

# Store the original psycopg2 module
original_psycopg2 = sys.modules.get('pg')
if not original_psycopg2:
    original_psycopg2 = importlib.import_module('pg')

# Save the original connect function
original_connect = original_psycopg2.connect

# Create a wrapper for the connect function
def connect_wrapper(*args, **kwargs):
    # Redirect connections to 192.168.44.4
    if 'host' in kwargs and kwargs['host'] == '192.168.44.4':
        print(f"Redirecting connection from 192.168.44.4 to 10.147.0.69")
        kwargs['host'] = '10.147.0.69'
    elif args and isinstance(args[0], str) and '192.168.44.4' in args[0]:
        # Handle connection string format
        args = list(args)
        args[0] = args[0].replace('192.168.44.4', '10.147.0.69')
        args = tuple(args)
    
    # Call the original connect function with modified arguments
    return original_connect(*args, **kwargs)

# Replace the original connect function with our wrapper
original_psycopg2.connect = connect_wrapper
