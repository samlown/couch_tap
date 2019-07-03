
require 'logging'


module CouchTap
  class OperationsQueue
    extend Forwardable

    def_delegators :@queue, :length, :pop

    def initialize(max_length, sleep_time = 1)
      @queue = Queue.new
      @max_length = max_length
      @sleep_time = sleep_time
    end

    def add_operation(op)
      while @queue.length >= @max_length
        logger.warn "Sleeping #{@sleep_time} after seeing #{@queue.length} operations in the queue!"
        sleep @sleep_time
      end
      @queue.push op
    end

    def close
      add_operation(Operations::CloseQueueOperation.new)
    end

    private

    def logger
      Logging.logger[self]
    end
  end
end

