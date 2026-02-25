"""
WSGI entry point for PythonAnywhere.
After cloning to PythonAnywhere, edit the path below to match your username.
"""
import sys
import os

# Change 'YOURUSERNAME' to your PythonAnywhere username
path = '/home/YOURUSERNAME/efpwealth'
if path not in sys.path:
    sys.path.insert(0, path)

# Set a strong secret key (change this to a random string)
if 'SECRET_KEY' not in os.environ:
    os.environ['SECRET_KEY'] = 'CHANGE-ME-TO-A-RANDOM-SECRET'

from app import app as application
