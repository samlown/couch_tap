
module CouchTap
  class OperationsQueue
    extend Forwardable

    def_delegators :@queue, :length, :pop

    def initialize
      @queue = Queue.new
    end

    def add_operation(op)
      @queue.push op
    end

    def close
      add_operation(Operations::CloseQueueOperation.new)
    end
  end
end

