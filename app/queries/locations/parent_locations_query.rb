# frozen_string_literal: true

module Locations
  class ParentLocationsQuery
    attr_reader :scope, :location

    def initialize(location:, scope: Location.all)
      @scope = scope
      @location = location
    end

    def self.call(location:)
      new(location:).call
    end

    def call
      return scope.none unless location

      sql = <<-SQL.squish
        WITH RECURSIVE recursive_locations AS (
          SELECT *
          FROM locations
          WHERE id = :location_id
          UNION ALL
          SELECT locations.*
          FROM locations
          INNER JOIN recursive_locations ON recursive_locations.location_id = locations.id
        )
        SELECT * FROM recursive_locations;
      SQL

      scope.find_by_sql([sql, { location_id: location.id }])
    end
  end
end
