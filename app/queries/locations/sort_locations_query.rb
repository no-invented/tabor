# frozen_string_literal: true

module Locations
  class SortLocationsQuery
    attr_reader :scope, :locations

    def initialize(locations:, scope: Location.all)
      @scope = scope
      @locations = locations
    end

    def self.call(locations:)
      new(locations:).call
    end

    def call
      location_ids = locations.select(&:id)
      return scope.none if location_ids.blank?

      location_types = scope.location_types.map(&:to_s)

      sql = <<-SQL.squish
        WITH RECURSIVE recursive_locations AS (
          SELECT *, 0 AS depth
          FROM locations
          WHERE location_id IS NULL
          UNION ALL
          SELECT locations.*, recursive_locations.depth + 1 AS depth
          FROM locations
          INNER JOIN recursive_locations ON locations.location_id = recursive_locations.id
        )
        SELECT *
        FROM recursive_locations
        WHERE id IN (:location_ids)
        ORDER BY
          depth ASC,
          array_position(ARRAY[:location_types], location_type::text),
          id ASC
      SQL

      scope.find_by_sql([sql, { location_ids: location_ids, location_types: location_types }])
    end
  end
end
