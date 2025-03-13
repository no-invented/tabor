# frozen_string_literal: true

describe Locations::SortLocationsQuery do
  subject(:query) { described_class }

  describe '.call' do
    subject(:query_call) { query.call(locations:) }

    let(:locations) { Location.all }

    context 'when locations are directly related' do
      let!(:region) { create(:location, location_type: :region) }
      let!(:city) { create(:location, location_type: :city, location_id: region.id) }
      let!(:district) { create(:location, location_type: :district, location_id: region.id) }

      it { is_expected.to contain_exactly(region, district, city) }
    end

    context 'when locations are not directly related' do
      let!(:district) { create(:location, location_type: :district) }
      let!(:region) { create(:location, location_type: :region) }
      let!(:city) { create(:location, location_type: :city) }

      it { is_expected.to contain_exactly(region, district, city) }
    end

    context 'with depth value' do
      let!(:region) { create(:location, location_type: :region) }
      let!(:city) { create(:location, location_type: :city, location_id: region.id) }
      let!(:district) { create(:location, location_type: :district, location_id: city.id) }
      let!(:region_second) { create(:location, location_type: :region) }
      let!(:street) { create(:location, location_type: :street, location_id: region_second.id) }
      let!(:address) { create(:location, location_type: :address, location_id: region.id) }

      it { is_expected.to contain_exactly(region, region_second, city, street, address, district) }
    end

    context 'without locations' do
      let(:locations) { Location.none }

      it { is_expected.to contain_exactly }
    end
  end
end
