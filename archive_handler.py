import gzip


class ArchiveHandler:

    @staticmethod
    def is_file_valid_warc_file(filename):
        if not filename.endswith('.gz') and not filename.endswith('.warc'):
            return False

        return True

    @staticmethod
    def get_file_from_archive(archive_location):
        gzip_file = gzip.open(archive_location, "rb")
        file_contents = gzip_file.read()

