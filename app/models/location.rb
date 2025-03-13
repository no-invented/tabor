# frozen_string_literal: true

class Location < ApplicationRecord
  belongs_to :parent_location,
             class_name: 'Location',
             foreign_key: 'location_id',
             inverse_of: :child_location,
             optional: true

  LOCATION_TYPES = %i[
    region
    district
    city
    street
    address
  ].freeze

  enum :location_type, LOCATION_TYPES

  def self.parent_locations(location)
    ::Locations::ParentLocationsQuery.call(location:)
  end

  def self.sort_locations(locations)
    ::Locations::SortLocationsQuery.call(locations:)
  end
end
