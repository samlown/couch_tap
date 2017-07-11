require 'test_helper'

module Destroyers
  class CollectionTest < Test::Unit::TestCase

    def setup
      @parent = mock()
      @queue = CouchTap::OperationsQueue.new(100_000)
      @metrics = CouchTap::Metrics.new
      @executor = CouchTap::QueryExecutor.new('changes', @queue, @metrics, db: 'sqlite:/')
    end

    def test_initialize_collection
      @collection = CouchTap::Destroyers::Collection.new(@parent) do
        # nothing
      end
      assert_equal @collection.parent, @parent
    end

    def test_raise_error_if_no_block
      assert_raise ArgumentError do
        @collection = CouchTap::Destroyers::Collection.new(@parent)
      end
    end

    def test_defining_table
      @table = mock()
      CouchTap::Destroyers::Table.expects(:new).with(@parent, :invoice_items, {}).returns(@table)
      @collection = CouchTap::Destroyers::Collection.new(@parent) do
        table :invoice_items
      end
      tables = @collection.instance_eval("@_tables")
      assert_equal tables.length, 1
      assert_equal tables.first, @table
    end

    def test_defining_tables
      CouchTap::Destroyers::Table.expects(:new).twice
      @collection = CouchTap::Destroyers::Collection.new(@parent) do
        table :invoice_items
        table :invoice_entries
      end
      tables = @collection.instance_eval("@_tables")
      assert_equal tables.length, 2
    end

    def test_execution
      @table = mock()
      CouchTap::Destroyers::Table.expects(:new).returns(@table)
      @collection = CouchTap::Destroyers::Collection.new(@parent) do
        table :invoice_items
      end
      @table.expects(:execute)
      @collection.execute(@executor)
    end
  end
end
