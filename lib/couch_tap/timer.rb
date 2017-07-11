
module CouchTap
  class Timer
    IDLE_STATUS = 0
    RUNNING_STATUS = 1

    attr_reader :status

    def initialize(timeout, &block)
      @timeout = timeout
      @block = block
      @status = IDLE_STATUS

      logger.debug "Timer configured with #{@timeout} s."
    end

    def run
      return unless @status == IDLE_STATUS
      @status = RUNNING_STATUS
      logger.debug "Timer starting!"
      @thread = Thread.new do
        begin
          sleep @timeout
          if @status == RUNNING_STATUS
            logger.debug "Timer firing!!"
            @block.call
          end
        end while @status == RUNNING_STATUS
      end
      @thread.abort_on_exception = true
    end

    # TODO we could create a halt method to halt the timer immediately
    def wait
      @status = IDLE_STATUS
      logger.debug "Waiting for timer to stop..."
      @thread.join
    end

    private

    def logger
      CouchTap.logger
    end
  end
end
