import sys
import os
import webbrowser
import pickle

chrome_path = 'C:/Program Files/Google/Chrome/Application/chrome.exe %s'
starturl = "https://www.indeed.com/viewjob?jk="
key_file = sys.argv[1]
with open(key_file, "rb") as f:
    key_list = pickle.load(f)

if __name__ == "__main__":
    os.system("start chrome")
    for key in key_list:
        url = starturl+key
        webbrowser.get(chrome_path).open(url, new=2)
    with open("viewed_jobs.pkl", "wb") as f:
        pickle.dump(key_list, f)