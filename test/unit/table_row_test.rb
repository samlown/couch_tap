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
    @row = CouchTap::TableRow.new(@handler, :items, doc['_id'], doc)

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
    id = '1234ABC'
    @database[:items].insert({:name => "An Item", :id => id})
    @row = CouchTap::TableRow.new(@handler, :items, id, {'_id' => id})

    assert_equal @row.attributes[:name], 'An Item'
  end

  def test_init_with_existing_row_and_updates
    doc = {'name' => 'Some Item', 'id' => '1234ABC'}
    @database[:items] << {'name' => "An Item", 'id' => '1234ABC'}
    @row = CouchTap::TableRow.new(@handler, :items, doc['id'], doc)

    assert_equal @row.attributes[:name], 'Some Item'
  end

  def test_execute_with_new_row
    time = Time.now
    doc = {'type' => 'Item', 'name' => "Some Item", '_id' => '1234'}
    @row = CouchTap::TableRow.new(@handler, :items, doc['_id'], doc)
    @row.execute

    items = @database[:items]
    item = items.first
    assert_equal items.where(:id => '1234').count, 1
    assert_equal item[:name], "Some Item"
  end

  def test_execute_with_new_row_with_time
    time = Time.now
    doc = {'type' => 'Item', 'name' => "Some Item", '_id' => '1234', 'created_at' => time.to_s}
    @row = CouchTap::TableRow.new(@handler, :items, doc['_id'], doc)
    @row.execute
    items = @database[:items]
    item = items.first
    assert item[:created_at].is_a?(Time)
    assert_equal item[:created_at].to_s, time.to_s
  end

  def test_execute_with_existing_row
    @database[:items] << {'name' => "An Item", 'id' => '1234ABC'}
    doc = {'type' => 'Item', 'name' => "Some Item", '_id' => '1234ABC'}
    @row = CouchTap::TableRow.new(@handler, :items, doc['_id'], doc)
    @row.execute

    assert_equal @database[:items].where(:id => '1234ABC').count, 1
    assert_equal @database[:items].first[:name], "Some Item"
  end

  def test_execute_with_deleted_doc
    row = {'name' => "An Item", 'id' => '1234ABC'}
    @database[:items] << row
    assert_equal @database[:items].where(:id => '1234ABC').count, 1

    # Perform the drop by only including the id
    @row = CouchTap::TableRow.new(@handler, :items, row['id'])
    @row.execute

    assert_equal @database[:items].where(:id => '1234ABC').count, 0
  end

  def test_column_assign_with_symbol
    doc = {'type' => 'Item', 'full_name' => "Some Other Item", '_id' => '1234'}
    @row = CouchTap::TableRow.new @handler, :items, doc['_id'], doc do
      column :name, :full_name
    end
    @row.execute

    data = @database[:items].first
    assert_equal data[:name], doc['full_name']
  end

  def test_column_assign_with_value
    doc = {'type' => 'Item', '_id' => '1234'}
    @row = CouchTap::TableRow.new @handler, :items, doc['_id'], doc do
      column :name, "Force the name"
    end
    @row.execute

    data = @database[:items].first
    assert_equal data[:name], "Force the name"
  end

  def test_column_assign_with_nil
    doc = {'type' => 'Item', 'name' => 'Some Item Name', '_id' => '1234'}
    @row = CouchTap::TableRow.new @handler, :items, doc['_id'], doc do
      column :name, nil
    end
    @row.execute
    data = @database[:items].first
    assert_equal data[:name], nil
  end

  def test_column_assign_with_block
    doc = {'type' => 'Item', '_id' => '1234'}
    @row = CouchTap::TableRow.new @handler, :items, doc['_id'], doc do
      column :name do
        "Name from block"
      end
    end
    @row.execute

    data = @database[:items].first
    assert_equal data[:name], "Name from block"
  end

  def test_column_assign_with_no_field
    doc = {'type' => 'Item', 'name' => "Some Other Item", '_id' => '1234'}
    @row = CouchTap::TableRow.new @handler, :items, doc['_id'], doc do
      column :name
    end
    @row.execute

    data = @database[:items].first
    assert_equal data[:name], doc['name']
  end


  protected

  def create_database
    database = Sequel.sqlite
    database.create_table :items do
      String :id
      String :name
      Time :created_at
      index :id, :unique => true
    end
    database
  end

end
