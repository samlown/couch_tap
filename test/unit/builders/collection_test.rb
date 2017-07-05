require 'test_helper'

module Builders
  class CollectionTest < Test::Unit::TestCase

    def setup
      @parent = mock()
      @queue = CouchTap::OperationsQueue.new(100_000)
      @metrics = CouchTap::Metrics.new
      @executor = CouchTap::QueryExecutor.new('changes', @queue, @metrics, db: 'sqlite:/')
    end

    def test_initialize_collection
      @collection = CouchTap::Builders::Collection.new(@parent, :items) do
        # nothing
      end
      assert_equal @collection.parent, @parent
      assert_equal @collection.field, :items
    end

    def test_raise_error_if_no_block
      assert_raise ArgumentError do
        @collection = CouchTap::Builders::Collection.new(@parent, :items)
      end
    end

    def test_defining_table
      @parent.expects(:data).returns({'items' => []})
      @collection = CouchTap::Builders::Collection.new(@parent, :items) do
        table :invoice_items do
          # nothing
        end
      end
    end

    def test_defining_table_with_items
      @parent.expects(:data).returns({'items' => [{'name' => 'Item 1'}]})
      CouchTap::Builders::Table.expects(:new).with(@parent, :invoice_items, {:data => {'name' => 'Item 1'}})
      @collection = CouchTap::Builders::Collection.new(@parent, :items) do
        table :invoice_items do
          # Nothing
        end
      end
    end

    def test_defining_table_with_items_using_symbols
      @parent.expects(:data).returns({'items' => [{:name => 'Item 1'}, {:name => 'Item 2'}]})
      CouchTap::Builders::Table.expects(:new).twice
      @collection = CouchTap::Builders::Collection.new(@parent, :items) do
        table :invoice_items
      end
    end

    def test_defining_table_with_null_data
      assert_nothing_raised do
        @parent.expects(:data).returns({'items' =>  nil})
        CouchTap::Builders::Table.expects(:new).never
        @collection = CouchTap::Builders::Collection.new(@parent, :items) do
          table :invoice_items
        end
      end
    end

    def test_defining_table_with_single_item
      @parent.expects(:data).returns({'item' => { name: 'Item 1' }})
      CouchTap::Builders::Table.expects(:new).with(@parent, :item, data: { name: 'Item 1'})
      CouchTap::Builders::Collection.new(@parent, :item) do
        table :item do
        end
      end
    end

    def test_execution
      table = mock()
      CouchTap::Builders::Table.expects(:new).twice.returns(table)
      @parent.expects(:data).returns({'items' => [{:name => 'Item 1'}, {:name => 'Item 2'}]})
      @collection = CouchTap::Builders::Collection.new(@parent, :items) do
        table(:invoice_items)
      end
      table.expects(:execute).twice
      @collection.execute(@executor)
    end

  end
end
