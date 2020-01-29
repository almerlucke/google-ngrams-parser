import re
import wget
import zipfile
import os
import logging

from parallel import run_parallel_batches

logging.basicConfig(level=logging.DEBUG)


def is_numeric(gram: str):
    try:
        int(gram)
        return True
    except ValueError:
        pass

    try:
        float(gram)
        return True
    except ValueError:
        pass

    return False


def is_valid_gram(gram: str):
    if is_numeric(gram):
        return False

    if gram.isalnum():
        return True

    return any(c.isalpha() for c in gram)


def download_ngram_file(url, tmp_dir):

    logging.info(f"start download url {url}")

    # get zip name from url
    zip_name = os.path.basename(url)

    # download zip to tmp
    zip_path = os.path.join(tmp_dir, zip_name)

    wget.download(url, zip_path)

    logging.info(f"unzip file {zip_path}")

    # extract zip
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(tmp_dir)

    # remove zip
    os.remove(zip_path)

    csv_path = os.path.join(tmp_dir, zip_name.replace(".zip", ""))

    return csv_path


def parse_gram_results(csv_files, gram_dictionary, ngram, cutoff_year):
    for csv_file in csv_files:
        logging.info(f"read csv {csv_file}")

        # read csv lines
        f = open(csv_file, "r")
        gram_lines = f.readlines()
        f.close()

        # remove csv file
        os.remove(csv_file)

        logging.info(f"parse ngram lines")

        # parse ngram lines
        for gram_line in gram_lines:
            components = gram_line.split("\t")
            gram = " ".join(components[:ngram]).lower()
            year = int(components[ngram])
            count = int(components[ngram + 1])

            if year > cutoff_year:
                if gram in gram_dictionary:
                    gram_dictionary[gram] += count
                else:
                    gram_dictionary[gram] = count


def parse_google_ngram_files(ngram: int, cutoff_year: int, max_entries: int, source_path: str, tmp_dir: str):
    url_seeker = re.compile("<a href='(.*?)'>")

    # open source html path
    f = open(source_path, "r")
    lines = f.readlines()
    f.close()

    jobs = []
    gram_dictionary = {}

    for line in lines:
        # get url from html line
        url = url_seeker.match(line).group(1)
        # append job
        jobs.append((download_ngram_file, (url, tmp_dir)))

    run_parallel_batches(jobs, 4, lambda results: parse_gram_results(results, gram_dictionary, ngram, cutoff_year))

    logging.info(f"sort entries")

    # get all entries, filter for valid grams and sort on frequency
    all_entries = list(gram_dictionary.items())
    all_entries = list(filter(lambda tup: is_valid_gram(tup[0]), all_entries))
    all_entries.sort(key=lambda tup: tup[1], reverse=True)

    return all_entries[:max_entries]


tmp_dir = "tmp"
dest_dir = "results"

if not os.path.exists(tmp_dir):
    os.mkdir(tmp_dir)

if not os.path.exists(dest_dir):
    os.mkdir(dest_dir)

entries = parse_google_ngram_files(2, 1980, 1_000_000, "sources/french_bigrams_sources.html", tmp_dir)

dest_path = os.path.join(dest_dir, "french_bigrams.csv")

logging.info(f"write entries to {dest_path}")

with open(dest_path, "w") as f:
    for entry in entries:
        f.write(f"{entry[0]}\t{entry[1]}\n")

