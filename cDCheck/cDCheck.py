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
            #print("adding to dup")
            if h in dup_file_dict:
                dup_file_dict[h].append(files[i])
            else:
                dup_file_dict[h] = [files[i], file_dict[h]]
        else:
            file_dict[h] = files[i]
            #print("adding to file_dict")

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
            if str(c).lower() == "s":
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
def checkPath(path, thread_count=4):
    file_dict = {} 
    dup_file_dict = {}
    file_count = 0
    files = []

    print("Processing files in directory: " + path)

    for i in os.listdir(path):
        if os.path.isfile(os.path.join(path,i)):
            file_count+=1
            files.append(os.path.join(path,i))
    print("Files found: " + str(file_count))

    threads = []
    f_slice = []

    #handle if we can do more threads than files
    if (thread_count > file_count):
        thread_count = file_count

    #starting per thread
    per_thread = int(file_count / thread_count)
    
    #set all threads
    for i in range(thread_count):
        f_slice.append(per_thread)

    #remainder number of files that haven't been distributed to threads
    extra_files = file_count - (per_thread * thread_count)

    #add remainder to threads as equally as possible
    for i in f_slice:
        if extra_files == 0:
            break
        i+=1
        extra_files -= 1

    #starts a thread_count threads
    #fill threads list with threads that we can start
    #f_slice is the number of files each thread should hash
    counter = 0
    for i in range(len(f_slice)):
        s1 = counter
        counter  = counter + f_slice[i]
        t = threading.Thread(target=processRange, args=(s1, counter, file_dict, dup_file_dict, files))
        threads.append(t)

    #start all threads
    for i in threads:
        i.start()

    #join all threads
    for i in threads:
        i.join()

    print("Done Processing Directory\n")

    callOutDups(dup_file_dict)

#entrant function
def main():
    #make sure we have enough args
    if len(sys.argv) >= 2:
        print("Please do not remove files from the given directory while this is running")

        path = sys.argv[1]

        #make sure path exists
        if os.path.exists(path):
            if (len(sys.argv) == 3):
                try:
                    t = int(sys.argv[2])
                    checkPath(path, t)
                except ValueError:
                    print("Number of threads is not an integer, please make it one and try again")
            if (len(sys.argv) == 2):
                checkPath(path)
        else:
            print("Given path does not exist, please check and try again")

    else:
        print("Usage: python cDCheck.py folderpath <number of threads, defaults to 4>")

if __name__ == '__main__':
    main()