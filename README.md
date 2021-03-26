# Ngrams
Utilities for downloading and reducing Google Ngram data

Because the Google Ngram data is so so large, I worked on ways to reduce it and make accessing it faster. 
1. I do not need the development of the usage of the word over time, rather, just the overall usage. So I sum the occurences for each year. The code is set up to only consider occurances after a given year, which is 0 AD by default.
2. Parts of speech are also not so important to me, so I reduce the number of entries even further. In fact, in the Ngram data any line which has an underscore in it can be neglected, because they are all redundant to one entry which has no POS indication. In the 3-gram series, for example, this leads to 10 entries becoming just 1, because you have 9 combinations with POS indicated.
3. I only keep 1-gram-like entries, and I squash them back into readable single-word format. For example, the 3-gram "cat ' s" becomes "cat's" and the 3 gram "my cat ate" is discarded.
At the end, I have a database that I can query for occurances for a given Ngram, but in a format that simplifies the occurances of what Google considers 3-grams into equivalent sensible 1-grams (see point 3. above). From about 1.22 TB of GZipped data I get a SQLite database of about 415 MB, which is managable.

Instructions for building a usable database from raw ngram data:

1. Download all ngram data into a folder using download_ngrams_v2020.py

2. Run ngramReduceParallel_v2020.py
     - Must specify source and target folder in code (e.g. eng or ger)

3. A .sqlite database appears in the database folder once done
     - Lookups like "SELECT count FROM lookup WHERE word = ?"
     - Can delete temp files like "reducedNgrams*"

## Relevant scripts

***ngramReduceParallel_v2020.py:***
Takes *.gz text files in specified folder. 
Sums all occurrences through time for all words. 
Creates text file reducedNgrams.txt with line format: [word]	[total_occurrences]\
Converts the text file into an indexed SQLite database for fast lookups.
- is not language-aware
- does not do any error checking of input data (e.g. does the word exist, or is it an OCR error?)
- keeps only 1-gram-like entries by default, e.g. "cat - people" -> "cat-people" and "her 's" -> "her's"
- All other ngrams are thrown away
- 4-grams would be completely ignored for example

***download_ngrams_v2020.py:***
Downloads ngram data automatically from Google using list of hard-coded URLs.
- Interruptable/restartable safe
- GZip files are downloaded sequentially

2021-03-25:	Updated to process new simpler v4 (2020) ngrams format
2019-09-25:	Changed to reading .gz directly, improved code speed
2018-12-10: 	Reorganized four scripts into this subfolder (dev/ngram), put two into (dev/ngram/old)
