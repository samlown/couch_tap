
module CouchTap
  class OperationsQueue
    extend Forwardable

    MAX_QUEUE_SIZE = 100_000

    def_delegators :@queue, :length, :pop

    def initialize(max_length = MAX_QUEUE_SIZE, sleep_time = 1)
      @queue = Queue.new
      @max_length = max_length
      @sleep_time = sleep_time
    end

    def add_operation(op)
      while @queue.length >= @max_length
        sleep @sleep_time
      end
      @queue.push op
    end

    def close
      add_operation(Operations::CloseQueueOperation.new)
    end
  end
end

