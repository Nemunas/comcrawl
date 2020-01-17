from comcrawl import IndexClient


def test_index_client():
    client = IndexClient(["2019-51"])
    assert len(client.indexes) == 1

    client = IndexClient(["2019-51", "2019-47"])
    assert len(client.indexes) == 2

    client = IndexClient()
    assert len(client.indexes) > 2