require 'test_helper'

class OperationsQueueTest < Test::Unit::TestCase

  class CouchTap::Operations::DummyOperation; end

  def test_queue_throttling
    @queue = CouchTap::OperationsQueue.new(1, 0.1)
    @queue.add_operation CouchTap::Operations::DummyOperation.new

    t = Thread.new do
      @queue.add_operation CouchTap::Operations::DummyOperation.new
      assert_equal 1, @queue.length
    end
    t.abort_on_exception = true

    sleep 0.1
    assert_equal 1, @queue.length
    @queue.pop
    @queue.pop
    @queue.close
  end
end

