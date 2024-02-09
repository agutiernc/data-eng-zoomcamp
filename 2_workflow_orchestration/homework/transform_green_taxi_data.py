if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    print(f'Preprocessing: rows with zero passengers => {data["passenger_count"].isin([0]).sum()}')
    print(f'Preprocessing: rows with zero passengers => {data["trip_distance"].isin([0]).sum()}')
    
    # add new column
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    # rename columns in camelCase to snake_case
    data.columns = (
        data.columns
            .str.replace('([a-z])([A-Z])', r'\1_\2', regex=True)
            .str.replace('([A-Z]+)([A-Z][a-z])', r'\1_\2', regex=True)
            .str.lower()
    )

    # return where the passenger count is greater than 0 and the trip distance is greater than 0
    return data[(data['passenger_count'] > 0) & (data['trip_distance'] > 0)]

@test
def test_output(output, *args) -> None:
    assert output['passenger_count'].isin([0]).sum() == 0, 'There are rides with zero passengers'
    assert output['trip_distance'].isin([0]).sum() == 0, 'There are rides with zerotrip distances'
    assert 'vendor_id' in output.columns, 'The "vendor_id" column is missing in the dataset'
