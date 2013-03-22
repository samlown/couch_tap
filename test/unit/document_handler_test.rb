
require 'test_helper'

class DocumentHandlerTest < Test::Unit::TestCase

  def test_init
    @handler = CouchTap::DocumentHandler.new 'changes' do
      #nothing
    end
    assert_equal @handler.changes, 'changes'
  end

  def test_execution
    @handler = CouchTap::DocumentHandler.new 'changes' do
      table :items
    end
    @handler.expects(:table).with(:items)

    doc = {'type' => 'Foo'}
    @handler.execute(doc)

    assert_equal @handler.document, doc
  end

  def test_table
    @handler = CouchTap::DocumentHandler.new 'changes' do
      # nothing
    end
    table = mock()
    table.expects(:save)
    CouchTap::TableRow.expects(:new).with(@handler, :items, nil, {}).returns(table)
    assert_nil @handler.table(:items)
  end
end
