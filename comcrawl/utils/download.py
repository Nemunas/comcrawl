"""Download Helpers.

This module contains helper functions for downloading
pages from the Common Crawl S3 Buckets.

"""

import io
import gzip
import requests
import tqdm
from ..types import Result, ResultList
from .multithreading import make_multithreaded


URL_TEMPLATE = "https://data.commoncrawl.org/{filename}"
MAX_RETRIES = 3


def download_single_result(result: Result) -> Result:
    """Downloads HTML for single search result.

    Args:
        result: Common Crawl Index search result from the search function.

    Returns:
        The provided result, extendey by the corresponding HTML String.

    """
    offset, length = int(result["offset"]), int(result["length"])

    offset_end = offset + length - 1

    url = URL_TEMPLATE.format(filename=result["filename"])
    tries = 0
    while tries < MAX_RETRIES:
        try:
            response = (requests
                        .get(
                            url,
                            headers={"Range": f"bytes={offset}-{offset_end}"},
                            timeout=30
                        ))
            break
        except (requests.exceptions.Timeout, requests.exceptions.RequestException) as e:
            tries += 1
            if tries <= MAX_RETRIES:
                print('An error occurred:', e, 'while trying to download', url)
                print('Retrying...')
            else:
                print('An error occurred:', e, 'while trying to download', url)                
                print('Max retries reached, skipping.')
                return result
            
    result["html"] = ""

    try:
        zipped_file = io.BytesIO(response.content)
        unzipped_file = gzip.GzipFile(fileobj=zipped_file)

        raw_data: bytes = unzipped_file.read()        
        data: str = raw_data.decode("utf-8")        

        if len(data) > 0:
            data_parts = data.strip().split("\r\n\r\n", 2)
            result["html"] = data_parts[2] if len(data_parts) == 3 else ""

    except Exception as e:
        print(f"Warning: Could not extract file downloaded from {url}", e)
        data = ""
        result['error'] = "Could not extract downloaded file. " + str(e)

    return result


def download_multiple_results(results: ResultList,
                              threads: int = None) -> ResultList:
    """Downloads search results.

    For each Common Crawl search result in the given list the
    corresponding HTML page is downloaded.

    Args:
        results: List of Common Crawl search results.
        threads: Number of threads to use for faster parallel downloads on
            multiple threads.

    Returns:
        The provided results list, extended by the corresponding
        HTML strings.

    """
    results_with_html = []

    # multi-threaded download
    if threads:
        multithreaded_download = make_multithreaded(download_single_result,
                                                    threads)
        results_with_html = multithreaded_download(results)

    # single-threaded download
    else:
        for result in tqdm.tqdm(results):
            result_with_html = download_single_result(result)
            results_with_html.append(result_with_html)

    return results_with_html
