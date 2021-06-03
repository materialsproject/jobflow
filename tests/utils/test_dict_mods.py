import pytest


def test_apply_mod():
    from jobflow.utils.dict_mods import apply_mod

    d = {"Hello": "World"}
    mod = {"_set": {"Hello": "Universe", "Bye": "World"}}
    apply_mod(mod, d)
    assert d == {"Bye": "World", "Hello": "Universe"}

    mod = {"_unset": {"Hello": 1}}
    apply_mod(mod, d)
    assert d == {"Bye": "World"}

    mod = {"_push": {"List": 1}}
    apply_mod(mod, d)
    assert d == {"Bye": "World", "List": [1]}

    mod = {"_push": {"List": 2}}
    apply_mod(mod, d)
    assert d == {"Bye": "World", "List": [1, 2]}

    mod = {"_inc": {"num": 5}}
    apply_mod(mod, d)
    assert d == {"Bye": "World", "List": [1, 2], "num": 5}

    mod = {"_inc": {"num": 5}}
    apply_mod(mod, d)
    assert d == {"Bye": "World", "List": [1, 2], "num": 10}

    mod = {"_rename": {"num": "number"}}
    apply_mod(mod, d)
    assert d == {"Bye": "World", "List": [1, 2], "number": 10}

    mod = {"_rename": {"num": "number2"}}
    apply_mod(mod, d)
    assert d == {"Bye": "World", "List": [1, 2], "number": 10}

    mod = {"_add_to_set": {"List": 2}}
    apply_mod(mod, d)
    assert d == {"Bye": "World", "List": [1, 2], "number": 10}

    mod = {"_add_to_set": {"List": 3}}
    apply_mod(mod, d)
    assert d == {"Bye": "World", "List": [1, 2, 3], "number": 10}

    e = {}
    mod = {"_add_to_set": {"List": 3}}
    apply_mod(mod, e)
    assert e == {"List": 3}

    mod = {"_add_to_set": {"number": 3}}
    with pytest.raises(ValueError):
        apply_mod(mod, d)

    mod = {"_pull": {"List": 1}}
    apply_mod(mod, d)
    assert d == {"Bye": "World", "List": [2, 3], "number": 10}

    mod = {"_pull": {"num": 1}}
    apply_mod(mod, d)
    assert d == {"Bye": "World", "List": [2, 3], "number": 10}

    mod = {"_pull": {"number": 3}}
    with pytest.raises(ValueError):
        apply_mod(mod, d)

    mod = {"_pull_all": {"List": [2, 3]}}
    apply_mod(mod, d)
    assert d == {"Bye": "World", "List": [], "number": 10}

    mod = {"_pull_all": {"number": 3}}
    with pytest.raises(ValueError):
        apply_mod(mod, d)

    mod = {"_push_all": {"List": list(range(10))}}
    apply_mod(mod, d)
    assert d == {"Bye": "World", "List": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9], "number": 10}

    e = {}
    apply_mod({"_push_all": {"k": list(range(3))}}, e)
    assert e == {"k": [0, 1, 2]}

    mod = {"_pop": {"List": 1}}
    apply_mod(mod, d)
    assert d == {"Bye": "World", "List": [0, 1, 2, 3, 4, 5, 6, 7, 8], "number": 10}

    mod = {"_pop": {"List": -1}}
    apply_mod(mod, d)
    assert d == {"Bye": "World", "List": [1, 2, 3, 4, 5, 6, 7, 8], "number": 10}

    mod = {"_pop": {"List": 2}}
    apply_mod(mod, d)
    assert d == {"Bye": "World", "List": [1, 2, 3, 4, 5, 6, 7, 8], "number": 10}

    mod = {"_pop": {"number": -1}}
    with pytest.raises(ValueError):
        apply_mod(mod, d)

    d = {}
    mod = {"_set": {"a->b->c": 100}}
    apply_mod(mod, d)
    assert d == {"a": {"b": {"c": 100}}}

    mod = {"_set": {"a->b->d": 200}}
    apply_mod(mod, d)
    assert d == {"a": {"b": {"c": 100, "d": 200}}}

    mod = {"_set": {"a->b->d": 300}}
    apply_mod(mod, d)
    assert d == {"a": {"b": {"c": 100, "d": 300}}}

    mod = {"_unset": {"a->b->d": 300}}
    apply_mod(mod, d)
    assert d == {"a": {"b": {"c": 100}}}

    mod = {"_push": {"a->e->f": 300}}
    apply_mod(mod, d)
    assert d == {"a": {"b": {"c": 100}, "e": {"f": [300]}}}

    mod = {"_push_all": {"a->e->f": [100, 200]}}
    apply_mod(mod, d)
    assert d == {"a": {"b": {"c": 100}, "e": {"f": [300, 100, 200]}}}

    mod = {"_inc": {"a->b->c": 2}}
    apply_mod(mod, d)
    assert d == {"a": {"b": {"c": 102}, "e": {"f": [300, 100, 200]}}}

    mod = {"_pull": {"a->e->f": 300}}
    apply_mod(mod, d)
    assert d == {"a": {"b": {"c": 102}, "e": {"f": [100, 200]}}}

    mod = {"_pull_all": {"a->e->f": [100, 200]}}
    apply_mod(mod, d)
    assert d == {"a": {"b": {"c": 102}, "e": {"f": []}}}

    mod = {"_push_all": {"a->e->f": [101, 201, 301, 401]}}
    apply_mod(mod, d)
    assert d == {"a": {"b": {"c": 102}, "e": {"f": [101, 201, 301, 401]}}}

    mod = {"_pop": {"a->e->f": 1}}
    apply_mod(mod, d)
    assert d == {"a": {"b": {"c": 102}, "e": {"f": [101, 201, 301]}}}

    mod = {"_pop": {"a->e->f": -1}}
    apply_mod(mod, d)
    assert d == {"a": {"b": {"c": 102}, "e": {"f": [201, 301]}}}

    mod = {"_abcd": {"a": "b"}}
    with pytest.raises(ValueError):
        apply_mod(mod, d)

    mod = {"_set": {"": ""}}
    with pytest.raises(TypeError):
        apply_mod(mod, d)
