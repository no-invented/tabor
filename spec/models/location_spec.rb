# frozen_string_literal: true

describe Location do
  it { expect(create(:location)).to be_valid }
  it { expect(create(:location, :with_parent)).to be_valid }
end
