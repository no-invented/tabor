# frozen_string_literal: true

class CreateLocations < ActiveRecord::Migration[8.0]
  def change
    create_table :locations do |t|
      t.integer 'location_type'
      t.integer 'location_id'

      t.timestamps
    end
  end
end
