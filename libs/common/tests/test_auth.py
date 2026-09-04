from clinical_common.auth import ROLE_ADMIN, ROLE_MEMBER, Principal


def test_internal_headers_roundtrip():
    p = Principal(user_id="u1", org_id="o1", role=ROLE_ADMIN)
    h = p.internal_headers("tok")
    assert h["X-Internal-Token"] == "tok"
    assert h["X-Principal-Org"] == "o1"
    assert p.is_admin


def test_member_is_not_admin():
    assert not Principal("u", "o", ROLE_MEMBER).is_admin
