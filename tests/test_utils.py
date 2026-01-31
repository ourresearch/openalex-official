"""Tests for utility functions."""

import pytest

from openalex_content.utils import (
    format_bytes,
    format_count,
    format_rate,
    parse_work_id,
    work_id_to_path,
)


class TestWorkIdToPath:
    def test_standard_work_id(self):
        path = work_id_to_path("W2741809807", "pdf")
        assert str(path) == "W27/41/W2741809807.pdf"

    def test_short_work_id(self):
        path = work_id_to_path("W123", "pdf")
        assert str(path) == "W01/23/W123.pdf"

    def test_xml_extension(self):
        path = work_id_to_path("W2741809807", "tei.xml")
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
