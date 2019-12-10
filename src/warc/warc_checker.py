from warcio.cli import Recompressor, Checker
from warcio.exceptions import ArchiveLoadFailed

import os
import re
import ntpath


class Command:
    def __init__(self, verbose, inputs):
        self.verbose = verbose
        self.inputs = inputs


WARC_FILE_REGEX = re.compile(r"(.*)(\.warc(.gz)?)")
WARC_RECOMPRESSED_ENDING = "_recompressed"


def is_file_valid_warc_file(filename):
    """
    Checks if the file has .warc or .gz file extension
    """
    if not filename.endswith('.warc.gz') and not filename.endswith('.warc'):
        return False
    return True


def is_warc_file_in_correct_format(filename):
    """
    Checks the payload and the block digests of the WARC record using the warcio cli command
    """
    try:
        print("Checking if the provided Warc file is in valid format")
        cmd = Command(False, filename)
        checker = Checker(cmd)
        checker.process_one(filename)
        return checker.exit_value == 0
    except ArchiveLoadFailed:
        return False


def recompress_warc_file_in_correct_format(filename):
    """
    Recompresses the provided .warc or .warc.gz file and creates a new .warc.gz file. If recompression fails,
    the whole program will exit as we cannot continue working with corrupted .warc files
    """
    recompression_result_file_location = get_new_recompression_file_location(filename)
    print("Recompressing file: %s to %s" % (filename, recompression_result_file_location))
    recompressor = Recompressor(filename, recompression_result_file_location)
    recompressor.recompress()
    return recompression_result_file_location


def get_new_recompression_file_location(filename):
    """
    Computes the name of the new recompressed filename by adding a _recompressed to the basename.
    """
    file_basename = ntpath.basename(filename)
    dirpath = ntpath.dirname(filename)

    match = WARC_FILE_REGEX.match(file_basename)
    filename = match.group(1)
    extension = match.group(2)

    recompression_file_name = filename + WARC_RECOMPRESSED_ENDING + extension
    recompression_file_location = os.path.join(dirpath, recompression_file_name)

    return recompression_file_location


