# Ngrams
Utilities for downloading and reducing Google Ngram data

Instructions for building a usable database from raw ngram data:

1. Download all ngram data into a folder using K:\NGRAM\eng using download_ngrams_v2020.py

2. Run ngramReduceParallel_v2020.py
     - Must specify source and target folder in code (e.g. eng or ger)

3. A .sqlite database appears in the database folder once done
     - Lookups like "SELECT count FROM lookup WHERE word = ?"
     - Can delete temp files like "reducedNgrams*"

## Relevant scripts

***ngramReduceParallel_v2020.py:***
Takes *.gz text files in specified folder
Sums all occurrences through time for all words
Creates text file reducedNgrams.txt with line format: [word]	[total_occurrences]
     - is not language-aware
     - does not do any error checking of input data (e.g. does the word exist, or is it an OCR error?)
     - keeps only 1-gram-like entries by default, e.g. "cat - people" -> "cat-people" and "her 's" -> "her's"
     - All other ngrams are thrown away
     - 4-grams would be completely ignored for example

***download_ngrams_v2020.py:***
Downloads ngram data automatically from Google using list of hard-coded URLs
Interruptable/restartable safe
GZip files are downloaded sequentially

2021-03-25:	Updated to process new simpler v4 (2020) ngrams format
2019-09-25:	Changed to reading .gz directly, improved code speed
2018-12-10: 	Reorganized four scripts into this subfolder (dev/ngram), put two into (dev/ngram/old)
