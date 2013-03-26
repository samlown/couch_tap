
require 'test_helper'

class DocumentHandlerTest < Test::Unit::TestCase

  def test_init
    @handler = CouchTap::DocumentHandler.new 'changes' do
      #nothing
    end
    assert_equal @handler.changes, 'changes'
  end

  def test_handles_with_basic_hash
    @handler = CouchTap::DocumentHandler.new 'changes', :type => 'Item'
    doc = {'type' => 'Item', '_id' => '1234'}
    assert @handler.handles?(doc)
    doc = {'type' => 'Client', '_id' => '1234'}
    assert !@handler.handles?(doc)
  end

  def test_handles_with_multi_level_hash
    @handler = CouchTap::DocumentHandler.new 'changes', :type => 'Item', :foo => 'bar'
    doc = {'type' => 'Item', 'foo' => 'bar', '_id' => '1234'}
    assert @handler.handles?(doc)
    doc = {'type' => 'Item', '_id' => '1234'}
    assert !@handler.handles?(doc)
    doc = {'foor' => 'bar', '_id' => '1234'}
    assert !@handler.handles?(doc)
  end

  def test_add
    @handler = CouchTap::DocumentHandler.new 'changes' do
      table :items
    end
    @handler.expects(:table).with(:items)
    doc = {'type' => 'Foo', '_id' => '1234'}
    @handler.add('1234', doc)
    assert_equal @handler.document, doc
    assert_equal @handler.id, '1234'
  end

  def test_drop
    @handler = CouchTap::DocumentHandler.new 'changes' do
      table :items
    end
    @handler.expects(:table).with(:items)
    @handler.drop('1234')
    assert_nil @handler.document
    assert_equal @handler.id, '1234'
  end

  def test_table
    @handler = CouchTap::DocumentHandler.new 'changes' do
      # nothing
    end
    table = mock()
    table.expects(:execute)
    CouchTap::TableRow.expects(:new).with(@handler, :items, nil, {}).returns(table)
    assert_nil @handler.table(:items)
  end
end
