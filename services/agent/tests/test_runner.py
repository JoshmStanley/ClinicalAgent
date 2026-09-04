from agent.runner import CITE_RE, build_messages, study_context_block


def test_build_messages_merges_same_role_and_starts_with_user():
    history = [
        {"role": "assistant", "text": "orphan"},
        {"role": "user", "text": "a"},
        {"role": "user", "text": "b"},
        {"role": "assistant", "text": "c"},
    ]
    msgs = build_messages(history)
    assert msgs[0] == {"role": "user", "content": "a\n\nb"}
    assert msgs[1]["role"] == "assistant"


def test_citation_regex():
    assert CITE_RE.findall("ORR was 41% [[chunk:doc1:3]] and [[chunk:doc2:0]].") == ["doc1:3", "doc2:0"]


def test_study_context_without_study():
    assert "not attached" in study_context_block({})
