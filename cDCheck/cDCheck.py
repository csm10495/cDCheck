#cDCheck: A Python script to check for, and delete duplicate files in a directory
#(C) Charles Machalow - MIT License 

import os        #for directory access
import sys       #for args
import threading #for threading

#processes the files in range
def processRange(r1, r2, file_dict, dup_file_dict, files):
    for i in range(r1, r2):
        #hash as binary
        h = hash(open(files[i], "rb").read())
        if h in file_dict:
            print("adding to dup")
            if h in dup_file_dict:
                dup_file_dict[h].append(files[i])
            else:
                dup_file_dict[h] = [files[i], file_dict[h]]
        else:
            file_dict[h] = files[i]
            print("adding to file_dict")

#alerts the user to duplicates
def callOutDups(dup_file_dict):
    for i in dup_file_dict:
        print("Duplicate file detected with hash: " + str(i))
        print("Instances:")

        for j, k in enumerate(dup_file_dict[i]):
            print(str(j) + ": " + str(k))
        
        #keep going to valid input
        while True:
            c = input("Choose a number for the file you would like to maintain. (s to skip this file)\n")
            
            #break character
            if c.lower() == "s":
                break
            
            try:
                c = int(c)
            except ValueError:
                print("Invalid input, choose one file (by number) to maintain")
                continue

            #make sure given int is valid
            if c >= 0 and c <= j:
                print("Performing requested action. Maintaining file " + str(c) + ". Deleting others.")
                for z in range(0, j + 1):
                    if z != c:
                        os.remove(dup_file_dict[i][z])
                break
            else:
                print("Invalid input, choose one file (by number) to maintain")

#does the iteration work
def checkPath(path):
    file_dict = {} 
    dup_file_dict = {}
    file_count = 0
    files = []
    for i in os.listdir(path):
        if os.path.isfile(os.path.join(path,i)):
            file_count+=1
            files.append(os.path.join(path,i))
    print("Files found: " + str(file_count))

    #hardcode 2 threads
    s1 = int(file_count / 2) - 1
    s2 = file_count
    t1 = threading.Thread(target=processRange, args=(0, s1, file_dict, dup_file_dict, files))
    t2 = threading.Thread(target=processRange, args=(s1, s2, file_dict, dup_file_dict, files))
    t1.start()
    t2.start()

    t1.join()
    t2.join()

    print("Done Processing Directory\n")

    callOutDups(dup_file_dict)

#entrant function
def main():
    #make sure we have enough args
    if len(sys.argv) == 2:
        print("Please do not remove files from the given directory while this is running")

        path = sys.argv[1]

        #make sure path exists
        if os.path.exists(path):
            checkPath(path)
        else:
            print("Given path does not exist, please check and try again")

    else:
        print("Usage: python cDCheck.py folderpath")

if __name__ == '__main__':
    main()