from statbank.api_exceptions import StatbankAuthError


def test_statbankautherror_param():
    err1 = StatbankAuthError(
        "Try adding the argument here",
        response_content={"ExceptionMessage": "You are using testcode bro"},
    )

    assert "ExceptionMessage" in err1.response_content
    assert len(err1.response_content.get("ExceptionMessage", ""))
