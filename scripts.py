import os

path = os.getcwd()

for files in os.walk(path):
    files = files[2]


