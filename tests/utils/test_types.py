def test_lenient_issubclass():
    from collections.abc import Mapping

    from pydantic import BaseModel

    from jobflow.core.schemas import JobStoreDocument
    from jobflow.utils.types import lenient_issubclass

    assert lenient_issubclass(int, int)
    assert not lenient_issubclass(str, int)
    assert lenient_issubclass(JobStoreDocument, BaseModel)

    # these cases will raise errors using issubclass
    assert not lenient_issubclass("test", str)
    assert not lenient_issubclass(list[str], Mapping)
    assert not lenient_issubclass("test", BaseModel)
    assert not lenient_issubclass(str, "not_a_class")
