require 'test_helper'

class SchemaTest < Test::Unit::TestCase

  def test_init
    database = create_database
    @schema = CouchTap::Schema.new(database, 'items')

    assert_equal @schema.name, :items
    assert_equal @schema.database, database
  end

  def test_init_when_table_does_not_exist
    database = Sequel.sqlite
    assert_raises Sequel::Error do
      CouchTap::Schema.new(database, :items)
    end
  end

  def test_dataset
    database = create_database
    @schema = CouchTap::Schema.new(database, 'items')
    database.expects(:[]).with(:items)
    @schema.dataset
  end

  def test_prepares_columns
    database = create_database
    @schema = CouchTap::Schema.new(database, 'items')
    assert_equal @schema.columns.keys, [:id, :name]
    obj = database.schema(:items)
    assert_equal @schema.columns.values, [obj[0][1], obj[1][1]]
  end

  def test_prepares_column_names
    database = create_database
    @schema = CouchTap::Schema.new(database, 'items')
    assert_equal @schema.column_names, [:id, :name]
  end

  protected

  def create_database
    database = Sequel.sqlite
    database.create_table :items do
      primary_key :id
      String :name
    end
    database
  end

end
