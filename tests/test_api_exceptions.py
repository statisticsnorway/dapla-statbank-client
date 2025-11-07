from statbank.api_exceptions import StatbankAuthError


def test_double_wrap_is_fine():
    err1 = StatbankAuthError("Try adding the argument here")
    err1.response_content = {"ExceptionMessage": "You are using testcode bro"}
    err2 = StatbankAuthError(err1)

    assert "ExceptionMessage" in err2.response_content
