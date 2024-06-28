import sys
import subprocess
import os

# get the least traffic
def query1(traffic=True):
    script_path = os.path.abspath("mongo_read.sh")
    arguments_low = input("Give low time (hh:mm:ss) :")
    arguments_great = input("Give great time (hh:mm:ss) :")
    # call the script with arguments
    process = subprocess.run([script_path] + [arguments_low , arguments_great ,str(traffic)])
    # check the return code
    if process.returncode == 0:
        print("Script ran successfully")
    else:
        print("Script failed with return code:", process.returncode)

# get the max distance
def query2():
    script_path = os.path.abspath("mongo_read_raw.sh")
    arguments_low = input("Give low time (hh:mm:ss) :")
    arguments_great = input("Give great time (hh:mm:ss) :")
    # call the script with arguments
    process = subprocess.run([script_path] + [arguments_low , arguments_great])
    # check the return code
    if process.returncode == 0:
        print("Script ran successfully")
    else:
        print("Script failed with return code:", process.returncode)

choice = input("Choose Query \n (1,2,3)")
if int(choice)==1:
    query1()
elif int(choice)==2:
    query1(traffic=False)
else:
    query2()
