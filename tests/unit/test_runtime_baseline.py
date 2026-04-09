from pathlib import Path

from janus.main import default_project_root
from janus.utils.runtime import (
    ICEBERG_CATALOG_IMPL,
    ICEBERG_SESSION_EXTENSIONS,
    build_spark_options,
    load_environment_config,
    materialize_runtime_paths,
    prepare_runtime,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
ICEBERG_RUNTIME_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1"


def test_default_project_root_prefers_current_working_directory(monkeypatch, tmp_path):
    monkeypatch.chdir(PROJECT_ROOT)
    monkeypatch.delenv("JANUS_PROJECT_ROOT", raising=False)

    assert default_project_root() == PROJECT_ROOT


def test_default_project_root_prefers_environment_variable(monkeypatch, tmp_path):
    monkeypatch.setenv("JANUS_PROJECT_ROOT", str(tmp_path))

    assert default_project_root() == tmp_path


def test_load_local_environment_uses_checked_in_defaults(monkeypatch):
    monkeypatch.delenv("JANUS_SPARK_MASTER", raising=False)
    monkeypatch.delenv("JANUS_RAW_DIR", raising=False)
    monkeypatch.delenv("JANUS_ICEBERG_RUNTIME_PACKAGE", raising=False)

    config = load_environment_config("local", PROJECT_ROOT)
    assert config["name"] == "local"
    assert config["spark"]["master"] == "local[*]"
    assert config["spark"]["ivy_dir"] == "data/metadata/ivy"
    assert config["spark"]["iceberg"]["catalog_name"] == "janus"
    assert config["spark"]["iceberg"]["runtime_package"] == ICEBERG_RUNTIME_PACKAGE
    assert config["storage"]["raw_dir"] == "data/raw"


def test_environment_variables_override_checked_in_defaults(monkeypatch):
    monkeypatch.setenv("JANUS_RAW_DIR", "/tmp/janus-raw")
    monkeypatch.setenv("JANUS_ICEBERG_WAREHOUSE_DIR", "/tmp/janus-iceberg")

    config = load_environment_config("local", PROJECT_ROOT)
    assert config["storage"]["raw_dir"] == "/tmp/janus-raw"
    assert config["spark"]["iceberg"]["warehouse_dir"] == "/tmp/janus-iceberg"


def test_prepare_runtime_creates_expected_directories(tmp_path):
    config = {
        "runtime": {"log_level": "INFO"},
        "spark": {
            "app_name": "janus-test",
            "master": "local[1]",
            "warehouse_dir": "data/metadata/spark-warehouse",
            "ivy_dir": "data/metadata/ivy",
            "iceberg": {
                "catalog_name": "janus",
                "warehouse_dir": "data/metadata/iceberg",
                "runtime_package": ICEBERG_RUNTIME_PACKAGE,
                "default_namespace": "bronze",
            },
            "config": {},
        },
        "storage": {
            "root_dir": "data",
            "raw_dir": "data/raw",
            "bronze_dir": "data/bronze",
            "metadata_dir": "data/metadata",
        },
    }

    paths = prepare_runtime(config, tmp_path)
    assert paths == materialize_runtime_paths(config, tmp_path)
    assert paths["raw_dir"].is_dir()
    assert paths["bronze_dir"].is_dir()
    assert paths["metadata_dir"].is_dir()
    assert paths["warehouse_dir"].is_dir()
    assert paths["ivy_dir"].is_dir()
    assert paths["iceberg_warehouse_dir"].is_dir()


def test_build_spark_options_merges_iceberg_runtime_settings(tmp_path):
    config = {
        "runtime": {"log_level": "INFO"},
        "spark": {
            "app_name": "janus-test",
            "master": "local[1]",
            "warehouse_dir": "data/metadata/spark-warehouse",
            "ivy_dir": "data/metadata/ivy",
            "iceberg": {
                "catalog_name": "janus",
                "warehouse_dir": "data/metadata/iceberg",
                "runtime_package": ICEBERG_RUNTIME_PACKAGE,
                "default_namespace": "bronze",
            },
            "config": {
                "spark.jars.packages": "com.example:demo:1.0.0",
                "spark.sql.extensions": "com.example.CustomSparkExtensions",
                "spark.sql.shuffle.partitions": "2",
            },
        },
        "storage": {
            "root_dir": "data",
            "raw_dir": "data/raw",
            "bronze_dir": "data/bronze",
            "metadata_dir": "data/metadata",
        },
    }

    paths = materialize_runtime_paths(config, tmp_path)
    options = build_spark_options(config, paths)

    assert options["spark.sql.warehouse.dir"] == str(paths["warehouse_dir"])
    assert options["spark.jars.ivy"] == str(paths["ivy_dir"])
    assert options["spark.jars.packages"] == (
        f"com.example:demo:1.0.0,{ICEBERG_RUNTIME_PACKAGE}"
    )
    assert options["spark.sql.extensions"] == (
        f"com.example.CustomSparkExtensions,{ICEBERG_SESSION_EXTENSIONS}"
    )
    assert options["spark.sql.defaultCatalog"] == "janus"
    assert options["spark.sql.catalog.janus"] == ICEBERG_CATALOG_IMPL
    assert options["spark.sql.catalog.janus.type"] == "hadoop"
    assert options["spark.sql.catalog.janus.warehouse"] == str(paths["iceberg_warehouse_dir"])
    assert options["spark.sql.catalog.janus.default-namespace"] == "bronze"
    assert options["spark.sql.shuffle.partitions"] == "2"
