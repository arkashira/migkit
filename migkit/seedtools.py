"""What the seed on media (`ship`, `carriers`) asks of each library it
wraps, against what each offers (backlog 0f): the compression of a part,
its encryption, and the object stores a carrier is reached through or
imported into.

Kept apart from the code it accounts for, so what is used is read off
that code (`tooluse.register(reads=...)`) and never off this module's own
names of it; only the reasons for the rest are written here.
"""
from . import tooluse

#: {command: {option: the token the code that drives it names it by}}
ZSTD = {"ZstdCompressor": {"level": "level",
                           "write_checksum": "write_checksum",
                           "stream_reader": "stream_reader"},
        "ZstdDecompressor": {"stream_reader": "stream_reader"}}
ZSTD_UNUSED = {
    ("ZstdCompressor", "threads"): "parts are compressed side by side, each"
                                   " on a thread of its own; a part's own"
                                   " threads would take the cores the"
                                   " others use",
    ("ZstdCompressor", "dict_data"): "a part is large enough that a"
                                     " dictionary trained on samples buys"
                                     " nothing",
    ("ZstdCompressor", "compression_params"): "the level says what the"
                                              " compressor does, and nothing"
                                              " measured asked for more",
    ("ZstdCompressor", "write_content_size"): "its default: a stream read in"
                                              " pieces says no size ahead",
    ("ZstdCompressor", "write_dict_id"): "no dictionary is used, so none is"
                                         " named in the frame",
    ("ZstdCompressor", "stream_writer"): "the encryption pulls the"
                                         " compressed bytes, so the"
                                         " compressor is read from, not"
                                         " written to",
    ("ZstdCompressor", "compress"): "a part is compressed as a stream as it"
                                    " is read, never whole in memory",
    ("ZstdCompressor", "copy_stream"): "see stream_writer",
    ("ZstdCompressor", "read_to_iter"): "see stream_writer",
    ("ZstdCompressor", "multi_compress_to_buffer"): "see compress",
    ("ZstdCompressor", "chunker"): "see compress",
    ("ZstdDecompressor", "dict_data"): "no dictionary is used when a part is"
                                       " written, so none is read",
    ("ZstdDecompressor", "max_window_size"): "its default reads every"
                                             " window the level a part is"
                                             " written at uses",
    ("ZstdDecompressor", "format"): "its default: every part is one whole"
                                    " frame, its magic number first",
    ("ZstdDecompressor", "decompress"): "a part is read as a stream as it is"
                                        " loaded, never whole in memory",
    ("ZstdDecompressor", "stream_writer"): "the load pulls the plain bytes,"
                                           " so the decompressor is read"
                                           " from, not written to",
    ("ZstdDecompressor", "read_to_iter"): "see stream_writer",
    ("ZstdDecompressor", "copy_stream"): "see stream_writer",
    ("ZstdDecompressor", "decompressobj"): "see stream_writer",
    ("ZstdDecompressor", "multi_decompress_to_buffer"): "see decompress",
}

AGE = {"": {"encrypt": "encrypt", "decrypt": "decrypt",
            "encrypt_io": "encrypt_io", "decrypt_io": "decrypt_io",
            "x25519": "x25519", "ssh": "ssh"}}
AGE_UNUSED = {
    ("", "encrypt_file"): "a part is encrypted as it is compressed, never"
                          " from a plain file on the disk",
    ("", "decrypt_file"): "a part is decrypted as it is loaded, never into"
                          " a plain file on the disk",
    ("", "passphrase"): "a carrier's parts are sealed to the recipients'"
                        " keys; a passphrase would have to travel beside"
                        " them",
    ("", "plugin"): "the identities are keys read from a file, which no"
                    " plugin is needed for",
}

S3 = {
    "client": {"endpoint_url": "endpoint_url", "region_name": "region_name",
               "verify": "verify", "profile_name": "profile_name",
               "addressing_style": "addressing_style", "retries": "retries"},
    "transfer": {"multipart_threshold": "multipart_threshold",
                 "multipart_chunksize": "multipart_chunksize",
                 "max_concurrency": "max_concurrency"},
    "calls": {c: c for c in ("upload_file", "download_file", "head_object",
                             "get_object", "put_object", "delete_object",
                             "list_objects_v2")},
}
S3_UNUSED = {
    ("transfer", "use_threads"): "its default: the pieces of one upload go"
                                 " side by side",
    ("transfer", "max_io_queue"): "its default: nothing measured asked for"
                                  " more",
    ("transfer", "io_chunksize"): "see max_io_queue",
    ("transfer", "num_download_attempts"): "its default: a part that could"
                                           " not be read is read again from"
                                           " the source",
    ("transfer", "max_bandwidth"): "the link a carrier sits on is its own;"
                                   " nothing is held back on it",
    ("transfer", "preferred_transfer_client"): "its default: the transfer"
                                               " the library chooses",
    ("calls", "head_bucket"): "the carrier is proved by a file written, read"
                              " back and taken away, which a bucket's head"
                              " does not prove",
    ("upload", "ChecksumAlgorithm"): "each part's sha256 is taken here and"
                                     " held at the far end, over bytes the"
                                     " endpoint cannot vouch for",
    ("upload", "SSECustomerKey"): "the parts are encrypted before they leave"
                                  " this machine, to keys the endpoint never"
                                  " holds",
    ("upload", "StorageClass"): "where the provider keeps the parts it"
                                " imports is its own choice",
}

BLOB = {
    "client": {"credential": "credential",
               "connection_verify": "connection_verify",
               "from_connection_string": "from_connection_string",
               "get_container_client": "get_container_client"},
    "calls": {c: c for c in ("upload_blob", "download_blob",
                             "get_blob_properties", "list_blobs",
                             "delete_blob", "readinto", "readall")},
    "upload": {"overwrite": "overwrite",
               "max_concurrency": "max_concurrency"},
}
BLOB_UNUSED = {
    ("upload", "standard_blob_tier"): "where the account keeps the parts is"
                                      " its own choice",
    ("upload", "validate_content"): "each part's sha256 is held at the far"
                                    " end, end to end, which a check of one"
                                    " request does not add to",
    ("upload", "metadata"): "the manifest says what each part is; a blob's"
                            " own metadata would say it twice",
    ("upload", "encryption_scope"): "the parts are encrypted before they"
                                    " leave this machine",
    ("upload", "cpk"): "see encryption_scope",
    ("upload", "tags"): "see metadata",
}

GCS = {"calls": {c: c for c in ("upload_from_filename",
                                "download_to_filename", "get_blob",
                                "list_blobs", "upload_from_string",
                                "download_as_bytes", "delete", "bucket",
                                "blob")}}
GCS_UNUSED = {
    ("upload", "checksum"): "its default: each upload is checked in flight,"
                            " and the part's sha256 is held at the far end"
                            " besides",
    ("upload", "if_generation_match"): "a part is written under a name of"
                                       " its own, once",
    ("upload", "content_type"): "the manifest says what each part is",
    ("upload", "timeout"): "its default: a part that could not be read is"
                           " read again from the source",
    ("upload", "retry"): "see timeout",
}

#: (name, licence, release, the files its uses are read off, used, unused)
ACCOUNTS = (
    ("zstandard", "BSD-3-Clause", "0.25", ("ship.py",), ZSTD, ZSTD_UNUSED),
    ("pyrage", "MIT", "1.4", ("ship.py", "evidence.py"), AGE, AGE_UNUSED),
    ("boto3-s3", "Apache-2.0", "1.43", ("carriers.py",), S3, S3_UNUSED),
    ("azure-storage-blob", "MIT", "12.30", ("carriers.py",), BLOB,
     BLOB_UNUSED),
    ("google-cloud-storage", "Apache-2.0", "3.15", ("carriers.py",), GCS,
     GCS_UNUSED),
)


def declared(used, unused):
    """What a library offers, as its release documents it: every call and
    option this account names, used or not."""
    out = {}
    for command, tokens in used.items():
        out.setdefault(command, set()).update(tokens)
    for command, option in unused:
        out.setdefault(command, set()).add(option)
    return {c: frozenset(o) for c, o in out.items()}


def _account():
    for name, licence, release, files, used, unused in ACCOUNTS:
        tooluse.register(
            name, licence=licence, release=release,
            declared=declared(used, unused),
            reads=(files, {(c, o): t for c, opts in used.items()
                           for o, t in opts.items()}),
            options={k: (tooluse.UNUSED, why) for k, why in unused.items()})


_account()
