# README

---
### Описание ТЗ
```ruby
create_table "locations", force: true do |t|
  t.integer "location_type"
  t.integer "location_id"
end


class Location < ActiveRecord::Base

  belongs_to :parent_location, class_name: 'Location', foreign_key: 'location_id'

  LOCATION_TYPES = [
    :region,
    :district,
    :city,
    :street,
    :address
  ]
  enum location_type: LOCATION_TYPES

  def self.parent_locations(location)
    #Реализовать метод класса Location, который на вход будет принимать экземпляр класса Location и будет возвращать на выходе массив экземпляров класса Location.
    #Результирующий массив должен содержать рекурсивно всех родителей location, включая сам location. Метод для получения родителя у location - parent_location.
  end

  def self.sort_locations(locations)
    #Реализовать метод сортировки массива экземляров класса Location.
    #Сортировка должна содержать следующие правила:
    #1) Если элементы экземляров класса Location между собой связаны через связь родитель-потомок, то родитель выводиться раньше потомка в сортировке. Связь родитель-потомок может в себе содержать больше двух элементов.
    #2) Если элементы экземляров класса Location между собой НЕ связаны через связь родитель-потомок, то выводиться тот элемент раньше, у которого location_type в массиве LOCATION_TYPES распроложен раньше.
    #3) Если в (2) вторая часть утверждения у пары элементов экземляров класса Location одинаковая и (1) невылнимо, то выводить тот элемент раньше, у которого глубина вложенности меньше.
    # Глубина вложенности - это количество родителей у элемента экземлляра класса Location.
  end
end
```
