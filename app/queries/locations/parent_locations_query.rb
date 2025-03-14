# frozen_string_literal: true

module Locations
  class ParentLocationsQuery
    attr_reader :location

    def initialize(location:)
      @location = location
    end

    def self.call(location:)
      new(location:).call
    end

    def call
      parents = []
      current_location = location

      while current_location
        parents << current_location
        current_location = current_location.parent_location
      end

      parents
    end
  end
end
