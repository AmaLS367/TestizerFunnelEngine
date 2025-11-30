from brevo.models import BrevoContact


def test_brevo_contact_to_payload_includes_required_fields() -> None:
    contact = BrevoContact(
        email="user@example.com",
        list_ids=[1, 2],
        attributes={"FUNNEL_TYPE": "language"},
    )

    payload = contact.to_payload()

    assert payload["email"] == "user@example.com"
    assert payload["updateEnabled"] is True
    assert payload["listIds"] == [1, 2]
    assert payload["attributes"] == {"FUNNEL_TYPE": "language"}


def test_brevo_contact_to_payload_omits_empty_lists_and_attributes() -> None:
    contact = BrevoContact(
        email="user@example.com",
        list_ids=[],
        attributes={},
    )

    payload = contact.to_payload()

    assert payload["email"] == "user@example.com"
    assert "listIds" not in payload
    assert "attributes" not in payload


def test_brevo_contact_to_payload_handles_none_attributes() -> None:
    contact = BrevoContact(
        email="user@example.com",
    )

    payload = contact.to_payload()

    assert payload["email"] == "user@example.com"
    assert "listIds" not in payload
    assert "attributes" not in payload
