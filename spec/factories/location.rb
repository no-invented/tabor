# frozen_string_literal: true

FactoryBot.define do
  factory :location do
    location_type { Location.location_types.keys.sample }

    trait :with_parent do
      location_id { create(:location).id }
    end
  end
end
