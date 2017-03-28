require 'test_helper'

module Builders
  class TableTest < Test::Unit::TestCase

    def setup
      @executor = CouchTap::QueryExecutor.new(db: 'sqlite:/')
      @database = initialize_database(@executor.database)
      @changes = mock()
      @changes.stubs(:database).returns(@database)
      @changes.stubs(:schema).returns(CouchTap::Schema.new(@database, :items))
      @handler = CouchTap::DocumentHandler.new(@changes)
    end

    def test_init
      doc = CouchRest::Document.new({'type' => 'Item', 'name' => "Some Item", '_id' => '1234'})
      @handler.document = doc
      @row = CouchTap::Builders::Table.new(@handler, 'items')

      assert_equal @row.parent, @handler
      assert_equal @row.handler, @handler
      assert_equal @row.document, doc
      assert_equal @row.name, :items

      assert_equal @row.primary_keys, [:item_id]

      # Also confirm that the automated calls were made
      assert_equal @row.attributes[:name], 'Some Item'
      assert_nil @row.attributes[:type]
      assert_nil @row.attributes[:_id]
      assert_equal @row.attributes[:item_id], '1234'

      assert_equal @row.instance_eval("@_collections.length"), 0
    end

    def test_init_with_data
      doc = CouchRest::Document.new({'type' => 'Group', 'name' => "Some Group", '_id' => '1234',
        'items' => [{'index' => 1, 'name' => 'Item 1'}]})
      @handler.document = doc
      @parent = CouchTap::Builders::Table.new(@handler, 'groups')
      @row = CouchTap::Builders::Table.new(@parent, 'items', :data => doc['items'][0])

      assert_equal @row.parent, @parent
      assert_equal @row.handler, @handler
      assert_equal @row.document, doc
      assert_equal @row.data, doc['items'][0]

      assert_equal @row.primary_keys, [:group_id, :item_id]
      assert_equal @row.attributes[:name], 'Item 1'
    end

    def test_init_with_primary_key
      doc = {'type' => 'Item', 'name' => "Some Item", '_id' => '1234'}
      @handler.document = doc
      @row = CouchTap::Builders::Table.new(@handler, :items, :primary_key => :entry_id)

      assert_equal @row.primary_keys, [:entry_id]
    end

    def test_init_with_data_string
      create_many_to_many_items
      doc = {'type' => 'Item', 'name' => "Some Group", '_id' => '1234',
        'item_ids' => ['i1234', 'i1235']}
      @handler.document = doc
      @parent = CouchTap::Builders::Table.new(@handler, 'groups')
      @row = CouchTap::Builders::Table.new(@parent, :group_items, :primary_key => false, :data => doc['item_ids'][0]) do
        column :item_id, data
      end
      @executor.row { @row.execute(@executor) }
      assert_equal @database[:group_items].first, {:group_id => '1234', :item_id => 'i1234'}
    end

    def test_execute_with_new_row
      doc = {'type' => 'Item', 'name' => "Some Item", '_id' => '1234'}
      @handler.document = doc
      @row = CouchTap::Builders::Table.new(@handler, :items)
      @executor.row { @row.execute(@executor) }

      items = @database[:items]
      item = items.first
      assert_equal items.where(:item_id => '1234').count, 1
      assert_equal item[:name], "Some Item"
    end

    def test_reuses_id
      doc = {'type' => 'Item', 'name' => "Some Item", '_id' => '1234'}
      @handler.document = doc
      @row = CouchTap::Builders::Table.new(@handler, :items)
      @executor.row { @row.execute(@executor) }

      items = @database[:items]
      item = items.first
      assert_equal items.where(:item_id => '1234').count, 1
      assert_equal item[:name], "Some Item"

      row = CouchTap::Builders::Table.new(@handler, :items)
      assert_raises Sequel::UniqueConstraintViolation do
        @executor.row { row.execute(@executor) }
      end
      assert_equal items.where(item_id: '1234').count, 1
    end

    def test_execute_with_new_row_with_time
      time = Time.now
      doc = {'type' => 'Item', 'name' => "Some Item", '_id' => '1234', 'created_at' => time.to_s}
      @handler.document = doc
      @row = CouchTap::Builders::Table.new(@handler, :items)
      @executor.row { @row.execute(@executor) }
      items = @database[:items]
      item = items.first
      assert item[:created_at].is_a?(Time)
      assert_equal item[:created_at].to_s, time.to_s
    end

    def test_building_collections
      doc = {'type' => 'Item', 'name' => "Some Group", '_id' => '1234',
        'items' => [{'index' => 1, 'name' => 'Item 1'}]}
      @handler.document = doc
      @row = CouchTap::Builders::Table.new @handler, :group do
        collection :items do
          # Nothing
        end
      end
      assert_equal @row.instance_eval("@_collections.length"), 1
    end

    def test_collections_are_executed
      @database.create_table :groups do
        String :item_id
        String :name
      end
      doc = {'type' => 'Item', 'name' => "Some Group", '_id' => '1234',
        'groups' => [{ 'group_id' => 1, 'name' => 'Group 1'}, {'group_id' => 2, 'name' => 'Group 2'}]}
      @handler.document = doc
      @row = CouchTap::Builders::Table.new @handler, :items do
        collection :groups do
          table :groups do
            # Nothing
          end
        end
      end
      @executor.row { @row.execute(@executor) }

      assert_equal @database[:items].where(item_id: '1234').count, 1
      assert_equal @database[:groups].where(item_id: '1234').count, 2
    end


    def test_column_assign_with_symbol
      doc = {'type' => 'Item', 'full_name' => "Some Other Item", '_id' => '1234'}
      @handler.document = doc
      @row = CouchTap::Builders::Table.new @handler, :items do
        column :name, :full_name
      end
      @executor.row { @row.execute(@executor) }

      data = @database[:items].first
      assert_equal data[:name], doc['full_name']
    end

    def test_column_assign_with_value
      doc = {'type' => 'Item', '_id' => '1234'}
      @handler.document = doc
      @row = CouchTap::Builders::Table.new @handler, :items do
        column :name, "Force the name"
      end
      @executor.row { @row.execute(@executor) }

      data = @database[:items].first
      assert_equal data[:name], "Force the name"
    end

    def test_column_assign_with_nil
      doc = {'type' => 'Item', 'name' => 'Some Item Name', '_id' => '1234'}
      @handler.document = doc
      @row = CouchTap::Builders::Table.new @handler, :items do
        column :name, nil
      end
      @executor.row { @row.execute(@executor) }
      data = @database[:items].first
      assert_equal data[:name], nil
    end

    def test_column_assign_with_empty_for_non_string
      doc = {'type' => 'Item', 'name' => 'Some Item Name', 'created_at' => '', '_id' => '1234'}
      @handler.document = doc
      @row = CouchTap::Builders::Table.new @handler, :items
      @executor.row { @row.execute(@executor) }
      data = @database[:items].first
      assert_equal data[:created_at], nil
    end

    def test_column_assign_with_integer
      doc = {'type' => 'Item', 'count' => 3, '_id' => '1234'}
      @handler.document = doc
      @row = CouchTap::Builders::Table.new @handler, :items
      @executor.row { @row.execute(@executor) }
      data = @database[:items].first
      assert_equal data[:count], 3
    end

    def test_column_assign_with_integer_as_string
      doc = {'type' => 'Item', 'count' => '1', '_id' => '1234'}
      @handler.document = doc
      @row = CouchTap::Builders::Table.new @handler, :items
      @executor.row { @row.execute(@executor) }
      data = @database[:items].first
      assert_equal data[:count], 1
    end

    def test_column_assign_with_float
      doc = {'type' => 'Item', 'price' => 1.2, '_id' => '1234'}
      @handler.document = doc
      @row = CouchTap::Builders::Table.new @handler, :items
      @executor.row { @row.execute(@executor) }
      data = @database[:items].first
      assert_equal data[:price], 1.2
    end


    def test_column_assign_with_float_as_string
      doc = {'type' => 'Item', 'price' => '1.2', '_id' => '1234'}
      @handler.document = doc
      @row = CouchTap::Builders::Table.new @handler, :items
      @executor.row { @row.execute(@executor) }
      data = @database[:items].first
      assert_equal data[:price], 1.2
    end


    def test_column_assign_with_block
      doc = {'type' => 'Item', '_id' => '1234'}
      @handler.document = doc
      @row = CouchTap::Builders::Table.new @handler, :items do
        column :name do
          "Name from block"
        end
      end
      @executor.row { @row.execute(@executor) }

      data = @database[:items].first
      assert_equal data[:name], "Name from block"
    end

    def test_column_assign_with_no_field
      doc = {'type' => 'Item', 'name' => "Some Other Item", '_id' => '1234'}
      @handler.document = doc
      @row = CouchTap::Builders::Table.new @handler, :items do
        column :name
      end
      @executor.row { @row.execute(@executor) }

      data = @database[:items].first
      assert_equal data[:name], doc['name']
    end


    protected

    def initialize_database(connection)
      connection.create_table :items do
        String :item_id
        String :name
        Integer :count
        Float :price
        Time :created_at
        index :item_id, :unique => true
      end
      connection
    end

    def create_many_to_many_items
      @database.create_table :group_items do
        String :group_id
        String :item_id
        index :group_id
      end
    end

  end
end
