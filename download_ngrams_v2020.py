# Automates download of ngram data v2020
import requests
import re
import os, fnmatch
from tqdm import tqdm
from urllib.parse import urlparse
import keyboard

def dl_list_from_html(source_URLs):
    all_HTML = ""
    for source in source_URLs:
        all_HTML += get_HTML_from_URL(source)
    return get_file_URLs_from_HTML(all_HTML, "\\.gz")

def get_HTML_from_URL(source):
    url = requests.get(source)
    return url.text

def get_file_URLs_from_HTML(html, file_filter):
    URLRegex = re.compile(r"<a href=\"((?!#)(?!\/).*?" + file_filter + r")\"")
    return URLRegex.findall(html)

def dl_list_from_raw_lists(URL_list, local_file_list):
    to_dl = []
    for URL in URL_list:
        required_URL = True
        for local_file in local_file_list:
            if URL.endswith(local_file):
                required_URL = False
        if required_URL:
            to_dl.append(URL)
    return to_dl

# From https://stackoverflow.com/questions/15644964/python-progress-bar-and-downloads
def dl_file(url, target_path, progress_marker=".inprogress", show_progress=True):

    fname = os.path.join(target_path, os.path.basename(url)) + progress_marker
    #keyboard.on_press_key("esc", lambda _:exit(1))
    resp = requests.get(url, stream=True)
    total = int(resp.headers.get('content-length', 0))
    with open(fname, 'wb') as file, tqdm(
        desc=fname,
        total=total,
        unit='iB',
        unit_scale=True,
        unit_divisor=1024,
    ) as bar:
        for data in resp.iter_content(chunk_size=1048576):
            size = file.write(data)
            bar.update(size)

    # Rename to remove progress_marker
    os.rename(fname, fname[:-len(progress_marker)])
    #keyboard.unhook_all()

if __name__ == '__main__':
    # Get indices of available files for 1, 2 and 3-grams
    source_URLs = ["http://storage.googleapis.com/books/ngrams/books/20200217/ger/ger-1-ngrams_exports.html",
                   "http://storage.googleapis.com/books/ngrams/books/20200217/ger/ger-2-ngrams_exports.html",
                   "http://storage.googleapis.com/books/ngrams/books/20200217/ger/ger-3-ngrams_exports.html"]
    raw_dl_list = dl_list_from_html(source_URLs)

    # Get current downloaded local file set
    downloads_path = "A:\\NGRAM\\Raw ngram data\\ger\\v2020"
    raw_local_list = fnmatch.filter(os.listdir(downloads_path), '*.gz')

    # Delete *.inprogress - failed downloads
    #del_local_inprogress(downloads_path)

    # Compare, generating list of required download URLs
    to_download_URLs = dl_list_from_raw_lists(raw_dl_list, raw_local_list)

    # Loop, downloading one at a time, giving async progress
    for ngram_URL in to_download_URLs:
        # Download to a local name filename.gz.inprogress
        dl_file(ngram_URL, downloads_path)
    


