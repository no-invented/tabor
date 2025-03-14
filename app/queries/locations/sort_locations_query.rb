# frozen_string_literal: true

module Locations
  class SortLocationsQuery
    attr_reader :locations

    def initialize(locations:)
      @locations = locations
    end

    def self.call(locations:)
      new(locations:).call
    end

    def call
      locations.sort_by do |location|
        [
          depth(location),
          Location.location_types.keys.index(location.location_type),
          location.id
        ]
      end
    end

    private

    def depth(location)
      depth = 0

      while location.parent_location
        depth += 1
        location = location.parent_location
      end

      depth
    end
  end
end
