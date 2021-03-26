#-------------------------------------------------------------------------------
# Name:        ngramReduce
# Purpose:      Reduce google ngram data into total count for all time and
#               part of speech
#               in:     word1[_POS1][ word2[_POS2]]  year    count   inbooks
#               out:    word1[ word2]    totalCount  POS2
#
# Author:      Kava
#
# Created:     24/11/2017
# Copyright:   (c) Kava 2017
# Licence:     irrelevant
#-------------------------------------------------------------------------------
# TODO: Data is extremely dirty. Also, somehow we get many entries for "company"
#       for example. See generated .sqlite file and run query:
#           select * from lookup where word is "company" order by OCC_RATIO desc
#       Could be that my assumption that the data is sorted is incorrect!
#           -Data is just really really dirty!!
#           Condensed data by simply adding non-unique entries using SQL
#               There were redundancies because of different parts of speech, etc.

import os, timeit, shutil, re, sys, io
import psutil
import sqlite3, timeit
import cProfile
from multiprocessing import Pool
from pprint import pprint as pprint

#sourcePath = os.getcwd()
#sourcePath = "A:/NGRAM/Raw ngram data/ger/v2020"
#targetPath = "A:/NGRAM/Raw ngram data/ger/v2020"
sourcePath = "X:/NGRAM/Raw ngram data/ger_v2020"
targetPath = "X:/NGRAM/Raw ngram data/ger_v2020"

def main():
    threadCount = psutil.cpu_count(logical = False)
    start_time = timeit.default_timer()
    # Get *.gz ngram file listing for directory
    ngramFiles = os.listdir(sourcePath)
    ngramFiles = [os.path.join(sourcePath, f) for f in ngramFiles if ".gz" in f]
    if ngramFiles:
        corruptZips = checkZipFilesIntegrity(ngramFiles)
        if corruptZips:
            print ("Corrupt zip files detected!")
            pprint(corruptZips)
            return
        if len(ngramFiles) == 1 or threadCount == 1:        # Useful for debugging
            results = [reduceNgrams("0", ngramFiles)]
        else:
            if len(ngramFiles) < threadCount:
                    threadCount = len(ngramFiles)
            #ngramFilesPartitioned = splitListLabel(ngramFiles, threadCount)
            #ngramFilesPartitioned = splitFileListLabelSize(ngramFiles, threadCount)
            print("Balancing load among " + str(threadCount) + " threads...")
            ngramFilesPartitioned = splitFileListLabelSizeEven(ngramFiles, threadCount)
            # Display workload distribution
            print(getLoadDist(ngramFilesPartitioned))

            # Distribute work to n PROCESSES! (use Pool, not ThreadPool...)
            pool = Pool(threadCount)
            results = pool.starmap(reduceNgrams, ngramFilesPartitioned)     # Fuck this code snippet: (".dummy"): https://stackoverflow.com/questions/5442910/python-multiprocessing-pool-map-for-multiple-arguments
            pool.close()
            pool.join()

        # Merge output files
        mergeFiles(results, os.path.join(targetPath, "reducedNgrams.txt"))

    elapsed = timeit.default_timer() - start_time
    print("Time taken: " + str(elapsed))

    buildDB()


# Reduces ngram data from google native format (entries sorted by year) into total occurences regardless of time
# ngramFiles is a list of gzip files which are unzipped on the fly
# Saves only detected equivalent 1-grams (and their counts) by default
# Sums instance counts for all years by default. Set minYear to e.g. 1960 for modern speach only.
import gzip
def reduceNgrams(label, ngramFiles, saveOnly1Grams = True, minYear = 0):
    """
    pr = cProfile.Profile()
    pr.enable()
    """
    ngramFiles.reverse()                # To get smallest files first (for better initial display of statistics)
    localInputFiles = len(ngramFiles)
    localCompletedInputFiles = 0
    start_time = timeit.default_timer()

    outputFileName = os.path.join(targetPath, "reducedNgrams_" + str(label) + ".txt")
    countFileName = os.path.join(targetPath, "reducedNgrams_" + str(label) + ".count")
    profileFileName = os.path.join(targetPath, "reducedNgrams_" + str(label) + ".cprofile")
    if os.path.isfile(countFileName) and os.path.isfile(outputFileName):
        print ("This processing thread (" + str(label) + ") is already done.\n")
        return outputFileName # Thread is already done
    with open(os.path.join(targetPath, "Worker_" + str(label) + "_workload.txt"), 'w', encoding='utf-8') as distroFile:
        distroFile.writelines(s + '\n' for s in ngramFiles)
    outputFile = open(outputFileName, "w", encoding="UTF-8")
    # Initialize stats collector
    globalTotalOccurrences = 0      # Total number of occurences for all words
    totalLinesProcessed = 0
    linesSkipped = 0

    # Get total size of files to process (for progress tracking)
    totalGBProcessed = 1e-10        # Avoid div0 error
    totalGB = 0
    for ngramFile in ngramFiles:
        totalGB += os.path.getsize(ngramFile) / 1e9

    # For each ngram file
    curFileDuration = 1
    curFileSize = 1
    occurences = 0
    for ngramFile in ngramFiles:
        lastFileMBps = curFileSize / curFileDuration / 1e6
        curFileStartTime = timeit.default_timer()

        with gzip.open(ngramFile, 'rt', encoding='utf-8') as curFile:
            # Read file contents line by line
            lastLine = ""
            for lineNum, line in enumerate(curFile):
                totalLinesProcessed += 1
                if ("_" in line or line == ""):  # Skip lines which have POS entries e.g. _ADJ_ (ALL POS-CONTAINING ENTRIES ARE REDUNDANT), or empty lines
                    linesSkipped += 1
                    continue

                splitLine = line.split("\t")
                rawWords = splitLine[0]
                cleanOutputWords = rawWords.split(" ")
                isCurWord1GramLike = is1GramLikeUsingIf(cleanOutputWords)
                if (not isCurWord1GramLike and saveOnly1Grams):     # Skip lines which aren't 1-gram like if we aren't going to save the results anyway
                    linesSkipped += 1
                    continue

                # Parse word_POS \t year,occurences,bookcount \t ... \n
                occurences = 0
                for record in splitLine[1:]:
                    splitRecord = record.split(',')
                    year = int(splitRecord[0])
                    if year >= minYear:
                        occurences += int(splitRecord[1])

                # Reassemble 1-grams and 1-gram-like entries without space
                if isCurWord1GramLike:
                    cleanOutputString = "".join(cleanOutputWords)   # No spaces - we only want 1-grams (e.g. "cat-like")
                # All others with space
                else:
                    cleanOutputString = " ".join(cleanOutputWords)   # Keep spaces - for "real" 2- and 3-grams (i.e. non-hyphenated and non-conjunction words) (e.g. "gut a fish")

                """ Helped discover that any line with any _POS tag is totally redundant and must be ignored
                if cleanOutputString == "zookeeper's":
                    with open("zookeeper's.log", "a") as zookeeperLogFile:
                        zookeeperLogFile.write(str(totalOccurences) + "\t" + ngramFile + ":\tline " + str(lineNum-1) + ":\t" + lastLine)
                elif cleanOutputString == "zookeeper":
                    with open("zookeeper.log", "a") as zookeeperLogFile:
                        zookeeperLogFile.write(str(totalOccurences) + "\t" + ngramFile + ":\tline " + str(lineNum-1) + ":\t" + lastLine)
                """

                # Save to file
                outputFile.write(cleanOutputString + "\t" + str(occurences) + "\n")

                if totalLinesProcessed % 500000 == 0:
                    GBps = lastFileMBps / 1000
                    GBremaining = totalGB - totalGBProcessed
                    ETA_hr = GBremaining / GBps / 3600             # in hours
                    if ETA_hr < 1:
                        ETA_string = "{:.2f}".format(ETA_hr*60) + " m"
                    elif ETA_hr > 24:
                        ETA_string = "{:.2f}".format(ETA_hr/24) + " d"
                    else:
                        ETA_string = "{:.2f}".format(ETA_hr) + " h"
                    try:    print("\tWorker " +str(label) + " ({:.4f}".format(totalGBProcessed * 100 / totalGB) + "%) ETA " + ETA_string + " ({:.2f}".format(GBps*1000) + " MB/s)" + ":\t" + ngramFile + ":\t" + str(lineNum) + ":\t" + rawWords, flush=True)
                    except: pass

            globalTotalOccurrences += occurences

        curFileSize = os.path.getsize(ngramFile)

        totalGBProcessed += curFileSize / 1e9
        outputFile.flush()
        os.fsync(outputFile.fileno())      # Flushes file writes to disk
        localCompletedInputFiles += 1
        curFileDuration = timeit.default_timer() - curFileStartTime

    # Close output file
    outputFile.close()
    print ("Processed lines:\t" + str(totalLinesProcessed))
    print ("Skipped lines:\t" + str(linesSkipped))
    print ("Kept occurences:\t" + str(globalTotalOccurrences))
    with open(countFileName, "w") as tcFile:
        tcFile.write("Processed lines:\t" + str(totalLinesProcessed) + "\n")
        tcFile.write("Skipped lines:\t" + str(linesSkipped) + "\n")
        tcFile.write("Kept occurences:\t" + str(globalTotalOccurrences) + "\n")
        for ngramFile in ngramFiles:
            tcFile.write("\t" + ngramFile + "\n")
        tcFile.flush()
        os.fsync(tcFile.fileno())
    """
    pr.disable()
    pr.print_stats()
    pr.dump_stats(profileFileName)
    """
    return outputFileName

# Takes an array of clean words (no _POS tags)
# Returns true if it looks like a 1-gram
#   cat's
#   cats'
#   cat 's
#   cat ' s
#   cat - like
# Equivalent (slower) regex: r"^((\S+ ('s|'ve|'d|'ll|'re|'m|' s|' ve|' d|' ll|' re|' m))|(\S+ - \S+)|(\S+))$"
def is1GramLikeUsingIf(words):
    return  ((len(words) == 3 and (words[1] == "-" or (words[1] == "'" and words[2] in ["s", "ve", "d", "ll", "re", "m"]))) # 3-gram hyphenated or conjunction
          or (len(words) == 2 and  words[1] in ["'s", "'ve", "'d", "'ll", "'re", "'m"])                                     # 2 gram conjunction
          or  len(words) == 1)                                                                                              # Any 1-gram

# Loop through words on this line (i.e. for 2+-grams) and clean parts of speech off
# Takes word content (e.g. "ghost_NOUN -_PRT like_ADJ")
# Returns words without POS tags, as an array (e.g. ["ghost", "-", "like"])
def cleanNgramPOS(rawWords):
    # posSplitter = re.compile(r"^(.+)_(?:.*)$|^(.+)")
    posSplitter = re.compile(r"^(.+)_.*$|^(.+)")
    cleanOutputWords = []
    splitWords = rawWords.split(" ")
    for dirtyWord in splitWords:
        splitPOS = posSplitter.search(dirtyWord)
        if not splitPOS:
            #skip the line, probably blank or misformatted
            return []
            break

        wordWithPos = splitPOS[1]
        wordNoPos = splitPOS[2]

        if wordNoPos:
            cleanWord = wordNoPos
        elif wordWithPos:
            cleanWord = wordWithPos
        else:
            #skip the line, probably blank or misformatted
            return []
            break

        cleanOutputWords.append(cleanWord)

    return cleanOutputWords

# Splits list a into n chunks
# https://stackoverflow.com/questions/2130016/splitting-a-list-into-n-parts-of-approximately-equal-length
def splitList(a, n):
    k, m = divmod(len(a), n)
    return list(a[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in xrange(n))

# Splits list into labeled tuple sublists for parallel mapping
# Takes list a and splits it into n chunks, each labeled 0...(n-1)
def splitListLabel(a, n):
    k, m = divmod(len(a), n)
    #return list((i, a[i * k + min(i, m):(i + 1) * k + min(i + 1, m)],) for i in range(n))
    return [(i, a[i * k + min(i, m):(i + 1) * k + min(i + 1, m)],) for i in range(n)]

# Splits a list of files split into n labeled lists based on file size
# Blindly distributes files sequentially
# Can lead to high STDDEV(size_distribution)
# [[0, [fileList0]], [1, [fileList1]], ...]
def splitFileListLabelSize(fileList, n):
    if not fileList:
        return None

    res = [ [i, []] for i in range(n) ]  # From https://stackoverflow.com/questions/33990673/how-to-create-a-list-of-empty-lists/33990750
    # Sort file list based on size
    sortedFiles = getFilesSortedBySize(fileList)
    # Rotating assignment into n lists
    curList = 0
    for file in sortedFiles:
        res[curList][1].append(file)
        curList += 1
        if curList > (n-1):
            curList = 0

    return res

# Splits a list of files into n labeled lists based on file size
# Distributes based on pipe-to-smallest-list approach
# [[0, [fileList0]], [1, [fileList1]], ...]
def splitFileListLabelSizeEven(fileList, n):
    if not fileList:
        return None

    res = [ [i, []] for i in range(n) ]  # From https://stackoverflow.com/questions/33990673/how-to-create-a-list-of-empty-lists/33990750
    # Sort file list based on size
    sortedFiles = getFilesSortedBySize(fileList)
    # Rotating assignment into n lists
    smallestList = 0
    for file in sortedFiles:
        res[smallestList][1].append(file)
        smallestList = getSmallestListIndex(res)

    return res

# Returns the index of the smallest list of files by size on disk
# listOfFileLists = [[0, [list of files]], [1, [list of files]], ...]
def getSmallestListIndex(listOfFileLists):
    # First quick pre-check for any empty lists
    for i in range(len(listOfFileLists)):
        curFileList = listOfFileLists[i][1]
        if not curFileList:
            return i            # This list is empty so just return it because you can't get any smaller...
    # No empty lists found, so must sort
    # Initialize smallest size to first entry
    indexOfSmallestSize = 0
    smallestSize = getFileListSizeOnDisk(listOfFileLists[indexOfSmallestSize][1])
    for i in range(len(listOfFileLists)):
        curSize = getFileListSizeOnDisk(listOfFileLists[i][1])
        if curSize < smallestSize:
            smallestSize = curSize
            indexOfSmallestSize = i

    return indexOfSmallestSize

# Returns size in bytes of a list of files
def getFileListSizeOnDisk(listOfFiles):
    sizeOnDisk = 0
    for f in listOfFiles:
        sizeOnDisk += os.path.getsize(f)
    return sizeOnDisk

# Returns files in size-sorted order (largest to smallest)
def getFilesSortedBySize(list, smallestToLargest = True):
    # Loop and add files to list.
    pairs = []
    for file in list:
        size = os.path.getsize(file)
        pairs.append([file, size])
    pairs.sort(key=lambda s: s[1])
    if smallestToLargest:
        pairs.reverse()
    sortedFiles = [row[0] for row in pairs]
    return sortedFiles

# https://stackoverflow.com/questions/13613336/python-concatenate-text-files
def mergeFiles(fileList, outputFile):
    with open(outputFile,'wb') as wfd:
        for f in fileList:
            with open(f,'rb') as fd:
                shutil.copyfileobj(fd, wfd, 1024*1024*10)
                #10MB per writing chunk to avoid reading big file into memory.

# Returns load distribution for each file group
def getLoadDist(fileGroups):
    res = []
    for group in fileGroups:
        size = 0
        for f in group[1]:
            size += os.path.getsize(f)
        res.append(size/(1000000000.))
    return res

# Builds a searchable database of ngram entries
def buildDB():
    global targetPath
    times = []
    times.append(["Start", timeit.default_timer()])
    # Open and initialize DB with table
    dbFileName = "ngrams.sqlite"
    con = sqlite3.connect(os.path.join(targetPath, dbFileName))
    cur = con.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS raw_lookup (uid TEXT UNIQUE, word TEXT, count INTEGER)")

    # Open source txt file
    times.append(["Read in text", timeit.default_timer()])
    cur.execute("BEGIN TRANSACTION")
    # Read processed n-grams into intermediate db table
    with open(os.path.join(targetPath, "reducedNgrams.txt"), "r", encoding="UTF-8") as ngramFile:
        # For each line in txt, enter into DB
        for lineNum, line in enumerate(ngramFile):
            splitLine = line.split("\t")
            word = splitLine[0].strip()
            occurences = int(splitLine[1])
            sql = '''INSERT OR IGNORE INTO raw_lookup (uid, word, count) VALUES (?, ?, ?)'''
            cur.execute(sql, [word + str(occurences), word, occurences])
            if lineNum % 100000 == 0:
                print(str(lineNum) + ": " + word)

    cur.execute("END TRANSACTION")

    # Create index on word in temp table
    times.append(["Index raw_lookup", timeit.default_timer()])
    print("Indexing raw lookup...\n")
    cur.execute("CREATE INDEX search ON raw_lookup (word)")

    # Once all in, condense entries such that all parts of speech contribute to the same count into new table
    times.append(["Condense values into lookup", timeit.default_timer()])
    cur.execute("CREATE TABLE lookup as SELECT word, SUM(count) AS count FROM raw_lookup GROUP BY word")
    # Delete temporary table raw_lookup
    times.append(["Delete raw_lookup", timeit.default_timer()])
    cur.execute("DROP TABLE raw_lookup")

    # Create index on word
    times.append(["Index lookup", timeit.default_timer()])
    print("Indexing...\n")
    cur.execute("CREATE UNIQUE INDEX search ON lookup (word)")
    # Vaccum & close DB
    print("Vacuuming...\n")
    times.append(["Cleanup", timeit.default_timer()])
    cur.execute("VACUUM")
    # Commit DB
    print("Committing...\n")
    times.append(["Save", timeit.default_timer()])
    con.commit()
    con.close()
    print("done.\n")
    times.append(["Done", timeit.default_timer()])

    pprint(times)


# Uzips a zip file and returns a list of files
import uuid
def unzipAndGetTempFiles(zipPath):
    # Make temp folder
    guid = uuid.uuid4()
    unzipPath = os.getcwd() + "/" + guid + "/"
    os.mkdir(unzipPath)
    # Unzip to temp folder
    unzip(zipPath, unzipPath)
    # Get file contents
    unzipedFiles = os.listdir(unzipPath)
    fullPaths = []
    for file in unzipedFiles:
        fullPaths.append(unzipPath + file)
    return fullPaths

import zipfile
def unzip(zipPath, targetDir):
    with zipfile.ZipFile(zipPath, 'r') as zip_ref:
        zip_ref.extractall(targetDir)

# Opens each zip file, then closes it
# checking for integrity
def checkZipFilesIntegrity(fileList):
    checkedZips = 0
    toCheck = len(fileList)
    corruptZips = []
    for file in fileList:
        try:
            with gzip.open(file, 'rt', encoding='utf-8') as curFile:
                xxx = curFile.readline()
                print ("Checked zip integrity of " + file)
        except:
            print ("Corrupt zip!: " + file)
            corruptZips.append(file)
        checkedZips += 1
        #print ("Zip checker: " + str(checkedZips*100/toCheck) + "%")
    return corruptZips


if __name__ == '__main__':
    main()

