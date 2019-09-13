from renku.core.models.mls import Run


def test_mls_serialization(mls):
    """Test mls (de)serialization."""
    mls_metadata = mls.asjsonld()
    print(mls_metadata)
    mls = Run.from_jsonld(mls_metadata)
    print(mls)

    # assert that all attributes found in metadata are set in the instance
    assert mls.identifier
    assert mls.executes
    assert mls.input_values
    assert mls.name

    # check values
    print(type(mls.executes), type(mls))
    assert mls.executes.implements._id == \
        mls_metadata.get('executes').get('implements').get('_id')
    assert mls.input_values[0].specified_by == \
        mls_metadata.get('input_values')[0].get('specified_by')
