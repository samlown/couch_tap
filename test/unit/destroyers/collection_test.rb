require 'test_helper'

module Destroyers
  class CollectionTest < Test::Unit::TestCase

    def setup
      @parent = mock()
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
      block = lambda do
        # nothing
      end
      CouchTap::Destroyers::Table.expects(:new).with(@parent, :invoice_items, {}, &block)
      @collection = CouchTap::Destroyers::Collection.new(@parent) do
        table :invoice_items, &block
      end
      tables = @collection.instance_eval("@_tables")
      assert_equal tables.length, 1
      assert tables.first.is_a?(CouchTap::Destroyers::Table)
    end

    def test_defining_tables
      @collection = CouchTap::Destroyers::Collection.new(@parent) do
        table :invoice_items
        table :invoice_entries
      end
      tables = @collection.instance_eval("@_tables")
      assert_equal tables.length, 2
      assert tables.last.is_a?(CouchTap::Destroyers::Table)
    end

    def test_execution
      @collection = CouchTap::Destroyers::Collection.new(@parent) do
        table :invoice_items
      end
      tables = @collection.instance_eval("@_tables")
      tables[0].expects(:execute)
      @collection.execute
    end

  end
end
