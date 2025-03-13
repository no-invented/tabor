# frozen_string_literal: true

describe Locations::ParentLocationsQuery do
  subject(:query) { described_class }

  describe '.call' do
    subject(:query_call) { query.call(location:) }

    context 'when location is exist' do
      let!(:region) { create(:location, location_type: :region) }
      let!(:district) { create(:location, location_type: :district, location_id: region.id) }
      let!(:city) { create(:location, location_type: :city, location_id: district.id) }
      let!(:street) { create(:location, location_type: :street, location_id: city.id) }
      let!(:address) { create(:location, location_type: :address, location_id: street.id) }

      context 'when location is district' do
        let(:location) { district }

        it { is_expected.to contain_exactly(district, region) }
      end

      context 'when location is address' do
        let(:location) { address }

        it { is_expected.to contain_exactly(address, street, city, district, region) }
      end
    end

    context 'without location' do
      let(:location) { nil }

      it { is_expected.to contain_exactly }
    end
  end
end
