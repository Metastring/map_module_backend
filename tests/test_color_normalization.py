import pytest

from register_dataset.api.utils import normalize_style_colors


def test_normalize_stroke_color():
    data = [{"stroke_color": "\\#FFFFFF"}]
    out = normalize_style_colors(data)
    assert out[0]["stroke_color"] == "#FFFFFF"


def test_normalize_custom_colors_and_mixed():
    data = [{"custom_colors": ["\\#abcabc", "#123456", None, 123], "stroke_color": None}, "other"]
    out = normalize_style_colors(data)
    assert out[0]["custom_colors"][0] == "#abcabc"
    assert out[0]["custom_colors"][1] == "#123456"
    assert out[0]["custom_colors"][2] is None
    assert out[0]["custom_colors"][3] == 123
    assert out[1] == "other"


def test_non_list_input():
    data = {"stroke_color": "\\#fff"}
    out = normalize_style_colors(data)
    assert out == data
