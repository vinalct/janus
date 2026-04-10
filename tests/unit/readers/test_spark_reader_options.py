from janus.readers import SparkDatasetReader


class _FakeReader:
    def __init__(self) -> None:
        self.format_name: str | None = None
        self.schema_value = None
        self.options: dict[str, str] = {}
        self.load_argument = None

    def format(self, format_name: str):
        self.format_name = format_name
        return self

    def schema(self, schema):
        self.schema_value = schema
        return self

    def option(self, key: str, value: str):
        self.options[key] = value
        return self

    def load(self, argument):
        self.load_argument = argument
        return self


class _FakeSpark:
    def __init__(self) -> None:
        self.read = _FakeReader()


def test_spark_dataset_reader_defaults_json_reads_to_multiline_documents():
    spark = _FakeSpark()

    reader = SparkDatasetReader().read_paths(
        spark,
        ["/tmp/example.json"],
        format_name="json",
    )

    assert reader.format_name == "json"
    assert reader.options == {"multiLine": "true"}
    assert reader.load_argument == "/tmp/example.json"


def test_spark_dataset_reader_keeps_jsonl_line_oriented_and_respects_explicit_options():
    spark = _FakeSpark()

    reader = SparkDatasetReader().read_paths(
        spark,
        ["/tmp/example.jsonl", "/tmp/example-2.jsonl"],
        format_name="jsonl",
        options={"samplingRatio": 0.5},
    )

    assert reader.format_name == "json"
    assert reader.options == {"samplingRatio": "0.5"}
    assert reader.load_argument == ["/tmp/example.jsonl", "/tmp/example-2.jsonl"]


def test_spark_dataset_reader_allows_explicit_json_options_to_override_defaults():
    spark = _FakeSpark()

    reader = SparkDatasetReader().read_paths(
        spark,
        ["/tmp/example.json"],
        format_name="json",
        options={"multiLine": "false"},
    )

    assert reader.options == {"multiLine": "false"}
