require 'test_helper'

class TableRowTest < Test::Unit::TestCase

  def setup
    @database = create_database
    @changes = mock()
    @changes.stubs(:database).returns(@database)
    @changes.stubs(:schema).returns(CouchTap::Schema.new(@database, :items))
    @handler = CouchTap::DocumentHandler.new(@changes)
  end

  def test_init
    doc = {'type' => 'Item', 'name' => "Some Item", '_id' => '1234'}
    @row = CouchTap::TableRow.new(@handler, :items, doc)

    assert_equal @row.handler, @handler
    assert_equal @row.document, doc
    assert_equal @row.name, :items

    # Also confirm that the automated calls were made
    assert_equal @row.attributes[:name], 'Some Item'
    assert_nil @row.attributes[:type]
    assert_nil @row.attributes[:_id]
    assert_nil @row.attributes[:id]
  end

  def test_init_with_existing_row
    @database[:items].insert({:name => "An Item", :id => '1234ABC'})
    @row = CouchTap::TableRow.new(@handler, :items, {'_id' => '1234ABC'})

    assert_equal @row.attributes[:name], 'An Item'
  end

  def test_init_with_existing_row_and_updates
    doc = {'name' => 'Some Item', 'id' => '1234ABC'}
    @database[:items] << {'name' => "An Item", 'id' => '1234ABC'}
    @row = CouchTap::TableRow.new(@handler, :items, doc)

    assert_equal @row.attributes[:name], 'Some Item'
  end


  def test_save_with_new_row
    doc = {'type' => 'Item', 'name' => "Some Item", '_id' => '1234'}
    @row = CouchTap::TableRow.new(@handler, :items, doc)
    @row.save

    assert_equal @database[:items].where(:id => '1234').count, 1
    assert_equal @database[:items].first[:name], "Some Item"
  end

  def test_save_with_existing_row
    @database[:items] << {'name' => "An Item", 'id' => '1234ABC'}
    doc = {'type' => 'Item', 'name' => "Some Item", '_id' => '1234ABC'}
    @row = CouchTap::TableRow.new(@handler, :items, doc)
    @row.save

    assert_equal @database[:items].where(:id => '1234ABC').count, 1
    assert_equal @database[:items].first[:name], "Some Item"
  end

  def test_column_assign_with_symbol

  end

  def test_column_assing_with_value

  end

  def test_column_assign_with_block

  end




  protected

  def create_database
    database = Sequel.sqlite
    database.create_table :items do
      String :id
      String :name
      index :id, :unique => true
    end
    database
  end

end
