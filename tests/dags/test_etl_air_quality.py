import os
import logging
from contextlib import contextmanager
import pytest
from airflow.models import DagBag
import json
from dags.etl_air_quality import extract_air_quality,transform_air_quality, load_air_quality

##############################

##############################

@contextmanager
def suppress_logging(namespace):
    logger = logging.getLogger(namespace)
    old_value = logger.disabled
    logger.disabled = True
    try:
        yield
    finally:
        logger.disabled = old_value


def get_import_errors():
    """
    Generate a tuple for import errors in the dag bag
    """
    with suppress_logging("airflow"):
        dag_bag = DagBag(include_examples=False)

        def strip_path_prefix(path):
            return os.path.relpath(path, os.environ.get("AIRFLOW_HOME"))

        # prepend "(None,None)" to ensure that a test object is always created even if it's a no op.
        return [(None, None)] + [
            (strip_path_prefix(k), v.strip()) for k, v in dag_bag.import_errors.items()
        ]


def get_dags():
    """
    Generate a tuple of dag_id, <DAG objects> in the DagBag
    """
    with suppress_logging("airflow"):
        dag_bag = DagBag(include_examples=False)

    def strip_path_prefix(path):
        return os.path.relpath(path, os.environ.get("AIRFLOW_HOME"))

    return [(k, v, strip_path_prefix(v.fileloc)) for k, v in dag_bag.dags.items()]


@pytest.mark.parametrize(
    "rel_path,rv", get_import_errors(), ids=[x[0] for x in get_import_errors()]
)
def test_file_imports(rel_path, rv):
    """Test for import errors on a file"""
    if rel_path and rv:
        raise Exception(f"{rel_path} failed to import with message \n {rv}")


APPROVED_TAGS = {}


@pytest.mark.parametrize(
    "dag_id,dag,fileloc", get_dags(), ids=[x[2] for x in get_dags()]
)
def test_dag_tags(dag_id, dag, fileloc):
    """
    test if a DAG is tagged and if those TAGs are in the approved list
    """
    assert dag.tags, f"{dag_id} in {fileloc} has no tags"
    if APPROVED_TAGS:
        assert not set(dag.tags) - APPROVED_TAGS


@pytest.mark.parametrize(
    "dag_id,dag, fileloc", get_dags(), ids=[x[2] for x in get_dags()]
)
def test_dag_retries(dag_id, dag, fileloc):
    """
    test if a DAG has retries set
    """
    assert (
        dag.default_args.get("retries", None) >= 2
    ), f"{dag_id} in {fileloc} must have task retries >= 2."




def test_extract_air_quality():
    
    with open('air_quality_data.json') as f:
        data = json.load(f)

    extract_data = extract_air_quality(data)
    assert isinstance(extract_data, dict)


def test_transform_air_quality():
    
    with open('air_quality_data.json') as f:
        data = json.load(f)

    transform_data = transform_air_quality(data)
    assert len(transform_data) > 0


def test_load_air_quality():
    
    with open('air_quality_data.json') as f:
        data = json.load(f)

    load_air_quality(data)
    # Add assertions here to verify the loaded data is correct
