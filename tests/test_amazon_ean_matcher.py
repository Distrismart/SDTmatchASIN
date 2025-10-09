from amazon_ean_matcher import extract_attribute_value


def test_extract_attribute_value_direct_string():
    attributes = {"Color": "Blue"}
    assert extract_attribute_value(attributes, ("color",)) == "Blue"


def test_extract_attribute_value_nested_mapping():
    attributes = {"ProductInfo": {"SizeName": {"value": "Large"}}}
    assert extract_attribute_value(attributes, ("size", "size_name", "sizename")) == "Large"


def test_extract_attribute_value_sequence():
    attributes = {
        "Details": [
            {"NumberOfItems": {"Values": ["24"]}},
            {"Fallback": "ignore"},
        ]
    }
    assert extract_attribute_value(attributes, ("number_of_items", "numberofitems")) == "24"


def test_extract_attribute_value_missing():
    attributes = {"SomethingElse": "value"}
    assert extract_attribute_value(attributes, ("color",)) is None
