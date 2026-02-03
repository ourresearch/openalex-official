"""Tests for utility functions."""

import pytest

from openalex_cli.utils import (
    doi_to_filename,
    format_bytes,
    format_count,
    format_rate,
    parse_identifier,
    parse_work_id,
    work_id_to_path,
)


class TestWorkIdToPath:
    def test_flat_default(self):
        """Default is flat (no nesting)."""
        path = work_id_to_path("W2741809807", "pdf")
        assert str(path) == "W2741809807.pdf"

    def test_flat_explicit(self):
        path = work_id_to_path("W2741809807", "pdf", nested=False)
        assert str(path) == "W2741809807.pdf"

    def test_nested_standard_work_id(self):
        path = work_id_to_path("W2741809807", "pdf", nested=True)
        assert str(path) == "W27/41/W2741809807.pdf"

    def test_nested_short_work_id(self):
        path = work_id_to_path("W123", "pdf", nested=True)
        assert str(path) == "W01/23/W123.pdf"

    def test_nested_xml_extension(self):
        path = work_id_to_path("W2741809807", "tei.xml", nested=True)
        assert str(path) == "W27/41/W2741809807.tei.xml"


class TestParseWorkId:
    def test_standard_id(self):
        assert parse_work_id("W2741809807") == "W2741809807"

    def test_url(self):
        assert parse_work_id("https://openalex.org/W2741809807") == "W2741809807"

    def test_numeric_only(self):
        assert parse_work_id("2741809807") == "W2741809807"

    def test_with_whitespace(self):
        assert parse_work_id("  W2741809807  ") == "W2741809807"


class TestParseIdentifier:
    def test_openalex_id(self):
        assert parse_identifier("W2741809807") == ("openalex", "W2741809807")

    def test_openalex_url(self):
        assert parse_identifier("https://openalex.org/W2741809807") == (
            "openalex",
            "W2741809807",
        )

    def test_doi_bare(self):
        assert parse_identifier("10.1038/nature12373") == ("doi", "10.1038/nature12373")

    def test_doi_url_https(self):
        assert parse_identifier("https://doi.org/10.1038/nature12373") == (
            "doi",
            "10.1038/nature12373",
        )

    def test_doi_url_http(self):
        assert parse_identifier("http://doi.org/10.1038/nature12373") == (
            "doi",
            "10.1038/nature12373",
        )

    def test_numeric_only(self):
        assert parse_identifier("2741809807") == ("openalex", "W2741809807")

    def test_with_whitespace(self):
        assert parse_identifier("  10.1038/nature12373  ") == (
            "doi",
            "10.1038/nature12373",
        )

    def test_invalid_identifier(self):
        with pytest.raises(ValueError, match="Cannot parse identifier"):
            parse_identifier("invalid-identifier")


class TestDoiToFilename:
    def test_standard_doi(self):
        assert doi_to_filename("10.1038/nature12373") == "10.1038_nature12373"

    def test_doi_with_multiple_slashes(self):
        assert (
            doi_to_filename("10.1000/journal.pone.0000000")
            == "10.1000_journal.pone.0000000"
        )

    def test_doi_with_colon(self):
        assert doi_to_filename("10.1234:example") == "10.1234_example"


class TestFormatBytes:
    def test_bytes(self):
        assert format_bytes(500) == "500.0 B"

    def test_kilobytes(self):
        assert format_bytes(1536) == "1.5 KB"

    def test_megabytes(self):
        assert format_bytes(2 * 1024 * 1024) == "2.0 MB"

    def test_gigabytes(self):
        assert format_bytes(1.5 * 1024 * 1024 * 1024) == "1.5 GB"


class TestFormatCount:
    def test_small_number(self):
        assert format_count(500) == "500"

    def test_thousands(self):
        assert format_count(5000) == "5.0K"

    def test_millions(self):
        assert format_count(2500000) == "2.5M"


class TestFormatRate:
    def test_bytes_per_second(self):
        assert format_rate(500) == "500.0 B/s"

    def test_megabytes_per_second(self):
        assert format_rate(2 * 1024 * 1024) == "2.0 MB/s"
